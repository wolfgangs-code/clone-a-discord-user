import sys
import json
import os
import argparse
import glob
import concurrent.futures
import re
from datetime import datetime

#?  Checks for bad/junk/useless messages that only add noise
def is_valid_text(text):
    #   Sanity checking
    if not text:
        return False
    clean_text = text.strip()
    if not clean_text:
        return False
    words = clean_text.split()
    if len(words) == 1:
        word = words[0]
        #   Check if the message is soley a mention, URL, or newline
        if word.startswith('@') or word.startswith('http://') or word.startswith('https://') or word.startswith('\n'):
            return False
    return True

def clean_discord_formatting(text):
    """Strips user/role mentions and custom/animated emojis, preserving standard text emojis."""
    if not text:
        return ""
    # Remove user and role mentions (matches <@12345>, <@!12345>, <@&12345>)
    text = re.sub(r'<@[!&]?\d+>', '', text)
    # Remove custom/animated emojis (matches <:name:12345> or <a:name:12345>)
    text = re.sub(r'<a?:\w+:\d+>', '', text)
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
    return text

def process_single_file(input_file, target_user_id, timeout_minutes, include_embeds):
    """Worker function to process a single file independently."""
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        return [], 0, 0, f"Failed to load {input_file}: {e}"

    raw_messages = data.get("messages", [])
    if not raw_messages:
        return [], 0, 0, f"No messages found in {input_file}"

    # --- PASS 1: Extract, Flatten, Merge, Assign Roles, and Track Media ---
    merged_messages = []
    current_msg = None

    for msg in raw_messages:
        msg_id = msg.get('id')
        author_data = msg.get('author', {})
        author_id = author_data.get('id')
        content = msg.get('content', '')
        ref_id = msg.get('reference', {}).get('messageId')
        timestamp_str = msg.get('timestamp')

        has_media = bool(msg.get('attachments')) or bool(msg.get('embeds'))

        if not author_id:
            continue

        author_id = int(author_id)

        if current_msg and current_msg['authorId'] == author_id:
            current_msg['content'] += f"\n{content}"
            # Track ALL original IDs in this merged block so replies to later messages still match
            current_msg['original_ids'].append(str(msg_id))
            current_msg['has_media'] = current_msg['has_media'] or has_media
            # Purposely NOT updating the timestamp here to avoid breaking timeout diffs
        else:
            if current_msg:
                merged_messages.append(current_msg)

            current_msg = {
                'messageId': str(msg_id),
                'original_ids': [str(msg_id)],
                'authorId': author_id,
                'content': content,
                'referenceId': str(ref_id) if ref_id else None,
                'timestamp': timestamp_str,
                'role': 'assistant' if author_id == target_user_id else 'user',
                'has_media': has_media
            }

    if current_msg:
        merged_messages.append(current_msg)

    # --- PASS 2: Synthesize Conversational Chains ---
    file_chains = []
    current_chain = []
    chain_user_id = None
    timeout_breaks = 0

    def save_current_chain(chain):
        # A valid training chain should end with an assistant response
        last_ast = -1
        for idx in range(len(chain) - 1, -1, -1):
            if chain[idx]["role"] == "assistant":
                last_ast = idx
                break

        # Must have at least one User -> Assistant turn
        if last_ast >= 1:
            valid_chain = chain[:last_ast + 1]
            formatted_thread = []

            for m in valid_chain:
                # 1. Strip URLs from assistant
                raw_content = re.sub(r'https?://\S+', '', m["content"]) if m["role"] == "assistant" else m["content"]
                # 2. Clean formatting
                cleaned = clean_discord_formatting(raw_content).strip()

                # 3. If any node in the chain is invalid (e.g., empty), discard the chain for quality
                if not is_valid_text(cleaned):
                    return False

                formatted_thread.append({
                    "role": m["role"],
                    "content": cleaned
                })

            file_chains.append({
                "id": valid_chain[0]["messageId"],
                "thread": formatted_thread
            })
            return True
        return False

    for i, msg in enumerate(merged_messages):
        added_to_chain = False

        if current_chain:
            last_msg = current_chain[-1]
            is_valid_next = False

            # Check Temporal/Reference continuity
            try:
                curr_time = datetime.fromisoformat(msg["timestamp"])
                last_time = datetime.fromisoformat(last_msg["timestamp"])
                diff_minutes = (curr_time - last_time).total_seconds() / 60.0
            except Exception:
                diff_minutes = 0

            time_ok = diff_minutes <= timeout_minutes
            ref_ok = (msg["referenceId"] in last_msg["original_ids"]) if msg["referenceId"] else False

            if time_ok or ref_ok:
                # Alternating role logic:
                if last_msg["role"] == "user" and msg["role"] == "assistant":
                    is_valid_next = True
                elif last_msg["role"] == "assistant" and msg["role"] == "user" and msg["authorId"] == chain_user_id:
                    is_valid_next = True

            # Ensure media constraints are respected
            if is_valid_next and not (not include_embeds and msg["has_media"]):
                current_chain.append(msg)
                added_to_chain = True
            elif not (time_ok or ref_ok) and msg["role"] == "user":
                 timeout_breaks += 1

        if not added_to_chain:
            # Chain broken: Save existing chain if it meets criteria
            if current_chain:
                save_current_chain(current_chain)
                current_chain = []

            # Start a new chain if the interrupting message is a valid User start
            if msg["role"] == "user" and not (not include_embeds and msg["has_media"]):
                current_chain = [msg]
                chain_user_id = msg["authorId"]

    # End of file catch-all
    if current_chain:
        save_current_chain(current_chain)

    return file_chains, len(raw_messages), timeout_breaks, None

def process_discord_data(input_files, target_user_id, timeout_minutes, max_threads, include_embeds):
    os.makedirs("paired", exist_ok=True)
    output_file = os.path.join("paired", f"{target_user_id}.json")

    all_new_chains = []
    total_raw_messages = 0
    total_timeout_breaks = 0

    print(f"Spawning pool with {max_threads} threads for {len(input_files)} files...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {
            executor.submit(process_single_file, f, target_user_id, timeout_minutes, include_embeds): f
            for f in input_files
        }

        for future in concurrent.futures.as_completed(futures):
            file_name = futures[future]
            try:
                chains, raw_count, breaks, err = future.result()
                if err:
                    print(f"[{file_name}] {err}")
                else:
                    all_new_chains.extend(chains)
                    total_raw_messages += raw_count
                    total_timeout_breaks += breaks
                    print(f"[{file_name}] Done. Found {len(chains)} conversation chains from {raw_count} messages.")
            except Exception as e:
                print(f"[{file_name}] Thread generated an exception: {e}")

    if total_raw_messages == 0:
        print("\nNo messages found in any of the provided files.")
        sys.exit(1)

    # --- PASS 3: Append, Deduplicate, and Sort (Synchronous) ---
    print("\nSynchronizing and merging into main dataset...")
    existing_chains = []
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                existing_chains = existing_data.get("conversations", [])
            print(f"Found existing '{output_file}' with {len(existing_chains)} chains.")
        except Exception as e:
            print(f"Warning: Could not read existing {output_file}. Starting fresh. Error: {e}")

    unique_chains = {}
    for chain in existing_chains:
        if "id" in chain:
            unique_chains[chain["id"]] = chain

    for chain in all_new_chains:
        unique_chains[chain["id"]] = chain

    final_chains = sorted(unique_chains.values(), key=lambda x: int(x["id"]))

    # --- SAVE FINAL OUTPUT ---
    with open(output_file, 'w', encoding='utf-8') as f:
        # Renamed array to 'conversations' to reflect the new structure
        json.dump({"conversations": final_chains}, f, indent=2, ensure_ascii=False)

    added_count = len(final_chains) - len(existing_chains)

    print(f"\n--- DONE ---")
    print(f"Processed {total_raw_messages} raw messages.")
    if not include_embeds:
        print("Embeds and attachments were filtered out.")
    print(f"Added {max(0, added_count)} new unique conversation chains.")
    print(f"Total dataset size: {len(final_chains)} chains.")
    print(f"Saved dataset to: {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Multithreaded processor for Discord JSON exports to Conversational Chains.")
    parser.add_argument("userid", type=int, help="The target user ID to train on")
    parser.add_argument("input_files", nargs="*", help="Path to raw Discord JSON files (defaults to all .json in cwd)")
    parser.add_argument("--timeout", type=int, default=10, help="Max minutes between messages to continue a chain (default: 10)")
    parser.add_argument("--include-embeds", action="store_true", help="Include pairs with embeds/attachments (default: exclude)")

    default_threads = min(32, (os.cpu_count() or 1) * 2)
    parser.add_argument("--threads", "-t", type=int, default=default_threads, help=f"Number of threads to use (default: {default_threads})")

    args = parser.parse_args()
    files_to_process = args.input_files

    if not files_to_process:
        files_to_process = glob.glob("*.json")
        if not files_to_process:
            print("Error: No input files provided and no .json files found in the current directory.")
            sys.exit(1)
        print(f"Found {len(files_to_process)} .json file(s) in the current directory.")

    process_discord_data(files_to_process, args.userid, args.timeout, args.threads, args.include_embeds)

if __name__ == "__main__":
    main()

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

    # --- PASS 2: Synthesize QA Pairs for this file ---
    # Map EVERY original message ID to its finalized merged block
    user_message_map = {}
    for msg in merged_messages:
        if msg["role"] == "user":
            for orig_id in msg["original_ids"]:
                user_message_map[orig_id] = {
                    "content": msg["content"],
                    "has_media": msg["has_media"]
                }

    file_pairs = []
    timeout_skipped = 0

    for i, msg in enumerate(merged_messages):
        if msg["role"] == "assistant":
            ast_has_media = msg["has_media"]
            user_content = None
            usr_has_media = False

            # Reference mapping logic (Now correctly checks all merged IDs)
            if msg["referenceId"]:
                ref_msg = user_message_map.get(msg["referenceId"])
                if ref_msg:
                    user_content = ref_msg["content"]
                    usr_has_media = ref_msg["has_media"]
            elif i > 0 and merged_messages[i - 1]["role"] == "user":
                prev_msg = merged_messages[i - 1]
                try:
                    ast_time = datetime.fromisoformat(msg["timestamp"])
                    usr_time = datetime.fromisoformat(prev_msg["timestamp"])
                    diff_minutes = (ast_time - usr_time).total_seconds() / 60.0

                    if diff_minutes <= timeout_minutes:
                        user_content = prev_msg["content"]
                        usr_has_media = prev_msg["has_media"]
                    else:
                        timeout_skipped += 1
                        continue
                except Exception:
                    # Fallback if timestamp parsing fails
                    user_content = prev_msg["content"]
                    usr_has_media = prev_msg["has_media"]

            # Skip this pair entirely if media is present and we aren't including them
            if not include_embeds and (ast_has_media or usr_has_media):
                continue

            if user_content is not None:
                # 1. Strip URLs from the assistant's raw message
                raw_assistant = re.sub(r'https?://\S+', '', msg["content"])

                # 2. Clean formatting and rigorously strip outer whitespace/newlines
                assistant_content = clean_discord_formatting(raw_assistant).strip()

                # 3. Clean user formatting and rigorously strip outer whitespace/newlines
                user_content = clean_discord_formatting(user_content).strip()

                # 4. Final validation before saving
                if is_valid_text(user_content) and is_valid_text(assistant_content):
                    file_pairs.append({
                        "id": msg["messageId"],
                        "user": user_content,
                        "assistant": assistant_content
                    })

    return file_pairs, len(raw_messages), timeout_skipped, None

def process_discord_data(input_files, target_user_id, timeout_minutes, max_threads, include_embeds):
    os.makedirs("paired", exist_ok=True)
    output_file = os.path.join("paired", f"{target_user_id}.json")

    all_new_pairs = []
    total_raw_messages = 0
    total_timeout_skipped = 0

    print(f"Spawning pool with {max_threads} threads for {len(input_files)} files...")

    # Execute file processing concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        # Map futures to filenames for error tracking
        futures = {
            executor.submit(process_single_file, f, target_user_id, timeout_minutes, include_embeds): f
            for f in input_files
        }

        for future in concurrent.futures.as_completed(futures):
            file_name = futures[future]
            try:
                pairs, raw_count, skipped, err = future.result()
                if err:
                    print(f"[{file_name}] {err}")
                else:
                    all_new_pairs.extend(pairs)
                    total_raw_messages += raw_count
                    total_timeout_skipped += skipped
                    print(f"[{file_name}] Done. Found {len(pairs)} pairs from {raw_count} messages.")
            except Exception as e:
                print(f"[{file_name}] Thread generated an exception: {e}")

    if total_raw_messages == 0:
        print("\nNo messages found in any of the provided files.")
        sys.exit(1)

    # --- PASS 3: Append, Deduplicate, and Sort (Synchronous) ---
    print("\nSynchronizing and merging into main dataset...")
    existing_messages = []
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                existing_messages = existing_data.get("messages", [])
            print(f"Found existing '{output_file}' with {len(existing_messages)} pairs.")
        except Exception as e:
            print(f"Warning: Could not read existing {output_file}. Starting fresh. Error: {e}")

    unique_pairs = {}

    for pair in existing_messages:
        if "id" in pair:
            unique_pairs[pair["id"]] = pair

    for pair in all_new_pairs:
        unique_pairs[pair["id"]] = pair

    final_messages = sorted(unique_pairs.values(), key=lambda x: int(x["id"]))

    # --- SAVE FINAL OUTPUT ---
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({"messages": final_messages}, f, indent=2, ensure_ascii=False)

    added_count = len(final_messages) - len(existing_messages)

    print(f"\n--- DONE ---")
    print(f"Processed {total_raw_messages} raw messages.")
    print(f"Filtered out {total_timeout_skipped} pairs due to the {timeout_minutes}-minute timeout.")
    if not include_embeds:
        print("Embeds and attachments were filtered out.")
    print(f"Added {max(0, added_count)} new unique QA pairs.")
    print(f"Total dataset size: {len(final_messages)} pairs.")
    print(f"Saved dataset to: {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Multithreaded processor for Discord JSON exports to QA pairs.")
    parser.add_argument("userid", type=int, help="The target user ID to train on")
    parser.add_argument("input_files", nargs="*", help="Path to raw Discord JSON files (defaults to all .json in cwd)")
    parser.add_argument("--timeout", type=int, default=10, help="Max minutes between messages to form a pair (default: 10)")
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

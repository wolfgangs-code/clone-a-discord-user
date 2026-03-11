# Clone a Discord User
Formerly *Big Grab*

----

*CaDU* Turns Discord chat exports into anonymized user-assistant pairs for LLM fine-tuning.

## Instruction.
```
python ./discord-clone.py <USERID> [--input-files <PATH:cwd>] [--timeout <MINS:10>] [--include-embeds <:false>]
```
When no `--input-files` argument is given, it will default to the current working directory.

## Cloning
The resulting output will be saved to `./paired/<USERID>.json`.
<!--TODO: Easy .ipynb -->

## Warning
This program **does not perform any in-message PII filtering** whatsoever.

There is a possibility that due to [overfitting](https://en.wikipedia.org/wiki/Overfitting#Consequences),
trained models **may** parrot input data when given its corresponding prompt.

As such, feeding DM's or private/secure channels into CaDU is considered *risky*, and is **not reccomended**.

# Twitter Wumao Finder

A "wumao" on twitter is not necessarily an employee of China's "grand external propaganda (Dawaixuan)", it could be a "little pink (xiaofenhong)", a far-leftist or any other kind of extremist. Blacklisting them. Do **not** waste time on scum. We all have our jobs to do, and our families to take care of.

## Testing

Tested on CPython 3.9.10.

## How it works

To make it simple and intuitive to operate without having to crawling huge amount of messages, the finder checks only twitter following relationship after adding initial seed accounts, and appends to list accounts with most wumao connections. Confirmed wumao accounts should also be evaluated after each iteration by calculating their internal folowing counts, and the evaluation result will be used to weight wumao connection during the next loop, so that the finder can be self-adaptive to varying groups of twitter users.

## Usage

1. Get twitter application tokens via [guide](https://python-twitter.readthedocs.io/en/latest/getting_started.html) of `python-twitter`, and change the configuration file `config.py` accordingly.
2. Add parameters such as token and project folder:

    ```sh
    tweeter-analyzer update-params --consumer-key YOUR_CONSUMER_KEY \
      --consumer-secret YOUR_CONSUMER_SECRET \
      --access-token YOUR_ACCESS_TOKEN \
      --access-token-secret YOUR_ACCESS_TOKEN_SECRET \
      --project-path YOUR_PROJECT_FOLDER_PATH
    ```

2. Initialize DB:

    ```sh
    tweeter-analyzer reset
    ```

3. Adding wumao seed accounts via account ID (not screen name):

    ```sh
    # e.g. adding accounts of People's Daily and Hu Xijin
    # usually it is not a good idea to add verified accounts as seed since their
    # followers are massive and the finder will lose direction ...
    tweeter-analyzer add-seed --seed 1531801543 --seed 2775998016
    ```

4. Automatically adding new wumao accounts:

    ```sh
    tweeter-analyzer calculate
    ```

5. Save wumao list to root as `wumao.csv`:

    ```sh
    tweeter-analyzer export --csv-path THE_OUTPUT_CSV_PATH
    ```

## Examples

It is not difficult to find some well-known wumaos. After exploring several banned list I added some seed and started the program for a while, and my initial [finding](./example.csv) is added as an example.

## Wumao Behavior Analysis

TBD

## Reference

1. https://python-twitter.readthedocs.io/en/latest/getting_started.html
2. https://github.com/bear/python-twitter/tree/master/examples
3. https://blog.yesmryang.net/wumao-twitter/

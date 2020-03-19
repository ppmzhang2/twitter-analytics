# Twitter Wumao Analysis

## Testing

Tested for pypy3.

## Find Newly Registered Wumao Twitter Accounts

Criteria:

* registered after 2020
* very active: statuses & liked tweets more than 500
* with less than 30 followers
* not protected
* accounts connected with each other by following

Search steps:

* initialize wumao list from followers of "grand external propaganda (Dawaixuan)" accounts
* check from potential wumao list each account's friends and followers, and keep those accounts with friends or followers in the list

## Wumao Behavior Analysis

TBD

## Usage

Populate potential wumao list with followers of a specific twitter account:

```sh
# search followers of People's Daily
python -m app save 1531801543
```

Keep only connected accounts in potential list:

```sh
python -m app validate
```

## Reference

1. https://python-twitter.readthedocs.io/en/latest/getting_started.html
2. https://github.com/bear/python-twitter/tree/master/examples
3. https://blog.yesmryang.net/wumao-twitter/

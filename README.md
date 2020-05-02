# Aggregate fields

Create an overview of a dataset by aggregating the possible variations from the selected fields
Useful for checking the consistency of data used together with the [Results Checker actor](https://apify.com/lukaskrivka/results-checker)

## Usage

Dataset format

```json
[{
  "type": "type-1",
  "categories": [
    "cat 1",
    "cat 2"
  ],
  "n": 1
},{
  "type": "type-2",
  "categories": [
    "cat 4",
    "cat 5"
  ],
  "n": 2
}]
```

INPUT

```json
{
  "datasetId": "defaultDatasetId",
  "fields": [
    "type",
    "categories",
    "n"
  ],
  "split": {
    "categories": " ",
    "type": "-"
  }
}
```

OUTPUT

```json
{
  "categories": {
    "values": [
      "cat",
      "1",
      "2",
      "4",
      "5"
    ],
    "count": 5,
    "min": 1,
    "max": 3,
    "average": 2
  },
  "type": {
    "values": [
      "type",
      "1",
      "2"
    ],
    "count": 3,
    "min": 1,
    "max": 4,
    "average": 2
  },
  "n": {
    "values": [
      1,
      2
    ],
    "count": 2,
    "min": 1,
    "max": 2,
    "average": 1
  }
}
```

## License

Apache 2.0

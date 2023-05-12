# Storage Writer

Write tables from input mapping to the target bucket in the destination project. You need to provide a Storage
token from the destination project which has `write` access to the target bucket **only**.

# Usage

Configuration:

```
{
	"storage": {
		"input": {
			"tables": [
				{
					"source": "in.c-main-some-table",
					"destination": "target-name"
				}
			]
		}
	}
	"parameters": {
		"#token": "some-token",
		"url": "https://connection.keboola.com/",
		"mode": "update"
	}
}
```

## Writing Mode
The writer supports three writing modes:

- `update` - will use [incremental loading](https://help.keboola.com/storage/tables/#incremental-loading) on the target table. 
- `replace` - will replace data in the target table. If the structures of the source and destination tables do not match, an error will be reported.
- `recreate` - will drop and create the target table. This will make sure that the structure of the destination table matches that of the source table.   

Default mode is `replace`. 

Note: Legacy configurations may have the `incremental` parameter. If it is true, it corresponds to the `update` mode. If it is false, it corresponds to the `replace` mode.

## Development

Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/wr-storage
cd wr-storage
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Run the test suite using this command:

```
docker-compose run --rm dev composer ci
```

The following environment variables have to be set:

- KBC_TEST_URL - URL of the destination Storage (e.g. https://connection.keboola.com/)
- KBC_TEST_BUCKET - Target bucket in the destination project
- KBC_TEST_TOKEN - Token to the destination project (with write access to the target bucket)

# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/)

## License

MIT licensed, see [LICENSE](./LICENSE) file.

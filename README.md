# Storage Writer

[![Build Status](https://travis-ci.org/keboola/my-component.svg?branch=master)](https://travis-ci.org/keboola/my-component)

Writes tables from input mapping to the configured bucket in the destination project. You need to provide a Storage
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
		"bucket": "in.c-target-bucket",
		"#token": "some-token",
		"url": "https://connection.keboola.com/"
	}
}
```

## Development
 
Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/my-component
cd my-component
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```

The following environment variables have to be set:

- KBC_TEST_URL - URL of the destination Storage (e.g. https://connection.keboola.com/)
- KBC_TEST_BUCKET - Target bucket in the destination project 
- KBC_TEST_TOKEN - Token to the destination project (with write access to the target bucket)

# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 

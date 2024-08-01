JOB_CONFIG_TEMPLATE = {
    "source": {
        "csv": {
            "csv": {"delimiter": ","},
            "tsv": {"delimiter": "\t"},
            "pipe": {"delimiter": "|"},
            "colon": {"delimiter": ":"},
        },
        "json": {},
        "jdbc": {},
    },
    "sink": {
        "csv": {
            "csv": {"delimiter": ","},
            "tsv": {"delimiter": "\t"},
            "pipe": {"delimiter": "|"},
            "colon": {"delimiter": ":"},
        },
        "json": {},
        "jdbc": {},
    },
}

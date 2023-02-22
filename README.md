# data-zoomcamp


## create deployment from cmd line:
```
prefect deployment build flows/web_to_gcs.py:etl_web_to_gcs \
  -n github-hw \
  -sb "github/zoom-github" \
  --apply
```
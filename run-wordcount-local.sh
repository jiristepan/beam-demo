python3 ./wordcount_gcp.py --input ./data/svejk.txt --output ./output/svejk  \
    --runner DirectRunner \
    --project activate-data \
    --region europe-west1 \
    --temp_locatio gs://activate-beam-demo/temp/ \
    --stagging_location gs://activate-beam-demo/staging \
    --job_name=job-svejk-bq \
    --usePublicIps=false --save_to_bq=True


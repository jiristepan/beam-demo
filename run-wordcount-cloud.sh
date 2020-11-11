python3 ./wordcount_gcp.py --input gs://activate-beam-demo/input/svejk.txt --output gs://activate-beam-demo/output/svejk  \
    --runner DataflowRunner \
    --project activate-data \
    --region europe-west1 \
    --temp_locatio gs://activate-beam-demo/temp/ \
    --stagging_location gs://activate-beam-demo/staging \
    --job_name=job-svejk \
    --usePublicIps=false \
    --save_to_bq=True


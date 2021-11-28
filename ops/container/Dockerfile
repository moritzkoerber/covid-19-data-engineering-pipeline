FROM public.ecr.aws/lambda/python:3.8

COPY handler.py ${LAMBDA_TASK_ROOT}

RUN pip3 config set --global global.no-cache-dir true
RUN pip3 install requests==2.26.0 pandas==1.3.4 s3fs --target "${LAMBDA_TASK_ROOT}"

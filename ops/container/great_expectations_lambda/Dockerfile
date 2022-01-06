FROM public.ecr.aws/lambda/python:3.8

COPY handler.py ${LAMBDA_TASK_ROOT}

RUN pip3 config set --global global.no-cache-dir true
RUN pip3 install great_expectations==0.13.49 --target "${LAMBDA_TASK_ROOT}"

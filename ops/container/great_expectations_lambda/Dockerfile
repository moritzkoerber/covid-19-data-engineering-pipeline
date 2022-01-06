FROM public.ecr.aws/lambda/python:3.8

COPY handler.py ${LAMBDA_TASK_ROOT}
COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN pip3 config set --global global.no-cache-dir true
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

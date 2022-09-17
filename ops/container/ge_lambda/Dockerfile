FROM public.ecr.aws/lambda/python:3.9

COPY handler.py ${LAMBDA_TASK_ROOT}
COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN pip3 install --no-cache-dir -r requirements.txt --target "${LAMBDA_TASK_ROOT}" \
    && rm -rf dataclasses*

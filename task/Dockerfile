# Dummy task for the fidelity experiments (Wayline backend).
#
# Originally eval/experiments/fidelity/task/Dockerfile in Nick's Wayline fork,
# built from the Wayline repo root. Now that task.py lives in saga-executors, the
# Wayline python SDK (sdk/python/wl) comes from a second, named build context.
# Build on cse-lambda1 from the saga-executors repo root:
#
#   sudo docker build -f task/Dockerfile \
#     --build-context wayline=/path/to/wayline \
#     -t localhost:5000/wl-fidelity-task:latest task/
#   sudo docker push localhost:5000/wl-fidelity-task:latest
#
# Use the Wayline commit that is deployed on the cluster (testbed/k8s/README.md).
# Requires BuildKit (docker >= 23 default) for --build-context.
FROM python:3.11-slim
WORKDIR /app
# The wl SDK imports pyzmq for its zeromq transport.
RUN pip install --no-cache-dir pyzmq
COPY --from=wayline sdk/python/wl ./wl
COPY task.py .
CMD ["python", "task.py"]

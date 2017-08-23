FROM cedadev/jasmin-sci-vm:latest

USER root

# Add Tini
ENV TINI_VERSION v0.15.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/local/bin/tini
RUN chmod +x /usr/local/bin/tini
ENTRYPOINT ["/usr/local/bin/tini", "--"]

# Create and become the dask user
RUN groupadd dask &&  \
    useradd -g dask -m -d /home/dask -s /sbin/nologin dask
USER dask

# Create a virtualenv from the JAP Python with dask and kubernetes client installed
RUN virtualenv --system-site-packages /home/dask/venv &&  \
    /home/dask/venv/bin/pip install dask[complete] distributed kubernetes

# Put the dask-scheduler and dask-worker executables on the path
RUN ln -s /home/dask/venv/bin/dask-scheduler /usr/local/bin/dask-scheduler
RUN ln -s /home/dask/venv/bin/dask-worker /usr/local/bin/dask-worker

COPY k8s-adaptive.py /home/dask/k8s-adaptive.py

# Run the dask scheduler using the kubernetes adaptive
CMD ["dask-scheduler", "--port 8786", "--bokeh-port 8787", "--preload /home/dask/k8s-adaptive.py"]

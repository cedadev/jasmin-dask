FROM cedadev/jasmin-sci-vm:latest

USER root

# Add Tini
ENV TINI_VERSION v0.15.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/local/bin/tini
RUN chmod +x /usr/local/bin/tini
ENTRYPOINT ["/usr/local/bin/tini", "-s", "--"]

# Create and become the dask user
RUN groupadd dask &&  \
    useradd -g dask -m -d /home/dask -s /sbin/nologin dask
USER dask

# Create a virtualenv from the JAP Python with dask and kubernetes client installed
# Install the latest versions of urllib3 and ipaddress to avoid SSL issues with kubernetes client
# Because the JAP Python isn't even the latest 2.7, we have to peg the bokeh version
# until 0.12.7 is out (see https://github.com/bokeh/bokeh/issues/6512)
# Tornado 4.5 doesn't work with bokeh 0.12.5, so we also have to peg that
# (see https://github.com/bokeh/bokeh/issues/6152)
RUN virtualenv --system-site-packages /home/dask/venv && \
    /home/dask/venv/bin/pip install dask[complete] distributed 'bokeh==0.12.5' 'tornado==4.4.3' kubernetes

USER root
# Put the dask-scheduler and dask-worker executables on the path
RUN ln -s /home/dask/venv/bin/dask-scheduler /usr/local/bin/dask-scheduler
RUN ln -s /home/dask/venv/bin/dask-worker /usr/local/bin/dask-worker
# Create the dask rundir and make it the working directory
RUN mkdir -p /var/run/dask && chown dask:dask /var/run/dask
WORKDIR /var/run/dask
USER dask

COPY k8s-adaptive.py /home/dask/k8s-adaptive.py

# Run the dask scheduler using the kubernetes adaptive config
CMD ["dask-scheduler", "--port", "8786", "--bokeh-port", "8787", "--preload", "/home/dask/k8s-adaptive.py"]

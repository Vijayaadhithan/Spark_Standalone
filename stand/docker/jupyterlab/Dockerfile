FROM cluster-base


ADD workspace/ ${SHARED_WORKSPACE}/

# -- Layer: JupyterLab


# -- Runtime

EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}
CMD jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=

FROM continuumio/miniconda3

VOLUME /secrets

# Set the working directory
WORKDIR /coinbasewebsocket-scraper

COPY environment.yml .
RUN conda env create -f environment.yml

#Activate the conda enviroment
RUN echo "conda activate web" > ~/.bashrc
ENV PATH /opt/conda/envs/web/bin:$PATH

COPY . .

CMD ["python", "coinbasewebsocket_scraper/scraper.py"]


FROM python:3.9.1
RUN apt-get update
RUN apt install nano & apt install less
ADD . /moralis-python-scripts
WORKDIR /moralis-python-scripts
RUN pip install -r requirements.txt

COPY entry_script.sh ./
RUN chmod 777 entry_script.sh


ENTRYPOINT ["/bin/bash", "entry_script.sh"]

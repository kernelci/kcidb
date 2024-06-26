FROM python:3.9
WORKDIR /app
COPY . ./
RUN pip3 install --break-system-packages .
ENV PYTHONUNBUFFERED True
ENTRYPOINT ["kcidb/cloud/iss-ed"]
CMD ["[]"]

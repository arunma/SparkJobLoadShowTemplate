FROM alpine:3.9.2

RUN echo "First docker container" > /tmp/echo.txt

CMD cat tmp/echo.txt


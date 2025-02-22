### data science image
FROM jupyter/scipy-notebook:python-3.7

### install some packages
RUN pip install pandas matplotlib seaborn
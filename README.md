# International Trade Indicator

## What it is

The goal of international trade indicator is to help people to understand and conduct macroeconomy research on international trade.

## How to use
<img src="https://github.com/Zalelin3/International_Trade_Indicator/blob/main/main.png" width="500" height="300">
<img src="https://github.com/Zalelin3/International_Trade_Indicator/blob/main/reference.png" width="500" height="300">

The main page UI interface support two ways to query the data from the database.

**Search through text bar**

To obtain the trade data, you should provide at least either the **name**, the **code**, or the **ISO3A's Code** of **two economies**, the **indicator's code**, and the **category's code** on the text bar. Additionally, you can type down two numbers or make selection on two year's button on the advance search section to enforce time constraint.
* Inputs are separated by semicolon.
* The first input Economic will be considered as the reporting economy.
* The order of the inputs does not matter.
* The text is case-insensitive.
* If there is no restriction on time, all possible results on the entire time range will be return.
* If only one time input is provided, it will be considered as the start year and return all possible values after that.

**Search through advance search**

* Selecting one of the options in Types of Reporting Economies or Partner Economy will update the options for Reporting Economy automatically.
* Selecting one of the options in Reporting Economies will update the options for Partner Economy automatically.
* Once Reporting Economy and Partner Economy are not none, all possible indicators will be presented.
* If Reporting Economy, Partner Economy, and Indicator are not none, all possible categories will be presented.

## Components

* Frontend: `Bootstrap`,`HTML`,`CSS`,`JS`,`AJAX`
* Backend: `FLASK`,`PYTHON`
* Database: `MySQL`
* Data Processor: `Apache Spark`
* Data Source: https://www.wto.org/english/res_e/statis_e/trade_datasets_e.htm

## How to set up

* Set up virtual environment

* Git clone to the directory

* pip3 install -r requirements.txt

* Open mysql workbench
  ```
  > mysql -u root -p
  > CREATE USER 'econ'@'localhost' IDENTIFIED BY 'econ';
  > CREATE DATABASE trade_db;
  > GRANT ALL PRIVILEGES ON trade_db . * TO 'econ'@'localhost';
  ```
* Install and set up Apache Spark

* Load the CSV files from WTO to MySQL

  Open terminal
  ```
  > spark-submit spark_transformer.py
  ```
* Run the application on development mode

  OS:
  ```
  > export FLASK_CONFIG=development
  > export FLASK_APP=run.py
  > flask run
  ```
  Windows:
  ```
  > set FLASK_CONFIG=development
  > set FLASK_APP=run.py
  > flask run
  ```

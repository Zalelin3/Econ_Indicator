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

# Master-Thesis

Note: The dataset file size exceeds the upload limit into the repository. To recreate the dataset, run the four .sas files and the .ipynb file in the data section.

## Code Structuring and Workflow

### Data (folder: Data/Final)
1.	data01_green_feng.sas constructs the 94 stock-level predictive characteristics. The SAS code is obtained from Jeremiah Green’s website (https://drive.google.com/file/d/0BwwEXkCgXEdRQWZreUpKOHBXOUU/view?resourcekey=0-1xjZ8fAc0sTybVC6RADDCA) and adopted to my specific features and timeframe.
2.	data02_reprisk.sas links the traditional dataset with RepRisk data.
3.	data03_cleaning.sas cleans the dataset.
4.	data04_imputation.sas imputes the missing data.
5.	data05-data07_prepare_sas_data_for_ml.ipynb provides some descriptive statistics and further preprocesses the dataset. Eventually, it creates the final dataset used in the empirical analysis (data07_model_input.parquet).

### Application of Machine Learning Models (folder: Code)
The machine learning models are applied in RF_trad.ipynb, RF_esg.ipynb, NN1_trad.ipynb, NN1_esg.ipynb, NN2_trad.ipynb, NN2_esg.ipynb, NN3_trad.ipynb, and NN3_esg.ipynb. The hyperparameters are tuned via 4-fold cross validation. An overview of the hyperparameter combinations and respective portfolio Sharpe ratios are saved in the results folder. Additionally, the out-of-sample predictions of the models with the best hyperparameter combination are saved in the results folder. The OLS-3 predictions are created in OLS-3.ipynb and saved in the results folder.
(Note: When rerunning the code, the predictions for the validation sample would be stored in a separate folder for each machine learning model. There, the trained neural networks would also be stored. However, these folders are not uploaded into the repository due to the size limit.)

### Empirical Analysis
The code in empirical_analysis.ipynb calculates the R2-scores, portfolio metrics, and performance metrics for predictions of each machine learning method. The results are saved in the empirical_analysis folder.

### Plots
The plots used in the thesis are created in Plots.ipynb.

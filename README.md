FALCON : Predicting Virality of Content for Condé Nast Brands
=============================================================

Predicting the virality of content data on social media for Condé Nast Brands. Code maintained by the Data Science team.

Requirements (assuming OSX): 
------------

1. Need to have protobuf installed, have tried it on Mac OSX. (brew install protobuf)
2. Need to have Go installed, for Vault tokens. `hvac` library
3. Also, install AWS CLI (brew install awscli)
4. make create_environment - Creates a datasci-virality conda environment
5. make requirements - After starting the conda environment
6. make MODE={prod/dev} update_settings_schema 
7. On the developer machine run: make MODE=prod update_settings_syncfroms3


Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── logs           <- Error and Information logs written by Falcon
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries for all Brands
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`. Also contains all notebooks for Spark Jobs
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials. To be updated !!!
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.testrun.org


Expansion to other Social Networks
------------

To come live soon: **Falcon on twitter**

1. Update the socialcopy collection and the connections with Socialflow. The main codes affected are in the folder
    * `settings.yaml` - Update the settings file to reflect twitter or other platform additions
    * `falcon/socialdata_utils/socialflow_utils_kafka.py` - Connections to Socialflow api for kafka based system
    * `falcon/socialdata_utils/socialflow_utils.py` - Connections to Socialflow api for presto based system
    * `falcon/data/socialcopy/socialposted_data_kafka.py` - Content URL cleanup for account types for Kafka based systems, relies on socialflow_utils
    * `falcon/data/socialcopy/socialposted_data.py` - Content URL cleanup for account types for Presto based systems, relies on socialflow_utils

2. Update all the databases for all brands to contain data for all social networks. Recommendations:
    * `falcon_later`: Stays the same with an additional column containing socialflow account page column, `facebook_page` or `twitter`
    * `falcon_never`: Stays the same with an additional column containing socialflow account page column, `facebook_page` or `twitter` 
    * `content_last_update_time`: Stays the same, presto last updated content data
    * `fbsocialcopy_last_update_time`: Change the table name to socialcopy_last_update_time and store another column of socialflow account page and update relevant row values
    * `model_outcomes` and `socialflow_queue_posted_data`: Create a separate table with prefix or suffix as the socialflow account page column which contains the relevant hourly recommendations for the account type
3. Create the training data which relies on the first 2 changes working fine.
4. Create the testing data for every hour which relies on the first 3 changes working fine and a trained model.


Contributing to Falcon
------------

**Rebase workflow followed for Falcon**
------------

Blog with details <a target="_blank" href="https://medium.com/singlestone/a-git-workflow-using-rebase-1b1210de83e5">Git Rebase Workflow for Falcon</a>

Slack @akatiyal for any issues regarding Falcon

--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>




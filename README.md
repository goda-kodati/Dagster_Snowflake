Dagster snowflake integration

--> Create a new folder Dagster_Snowflake 

--> Create a virtual environment E:\Dagster_Snowflake>virtualenv venv

--> Activate the virtual environment  E:\Dagster_Snowflake>venv\Scripts\activate

--> Install dagster (venv) E:\Dagster_Snowflake>pip install dagster

--> Create a dagster project (venv) E:\Dagster_Snowflake>dagster project scaffold --name integer-squawk-id-project

--> Install dependent packages:

(venv) E:\Dagster_Snowflake>pip install dagit==1.1.15 (venv) E:\Dagster_Snowflake>pip install dagster-snowflake==0.17.15

--> Write your code in assets.py(created by default)

--> Command to run the assets.py: (venv) E:\Dagster_Snowflake>dagit -f integer-squawk-id-project\integer_squawk_id_project\assets.py

--> Generate requirements.txt file using the following command: (venv) E:\Dagster_Snowflake>pip freeze > requirements.txt

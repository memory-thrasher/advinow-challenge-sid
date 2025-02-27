# AdviNow Interview Challenge
This repository will be used as a test case for interview candidates. The application structure is predefined using FastAPI and uvicorn in the "app" directory in the "run.py" module. 
FastAPI creates API docs automatically, and these can be found at "http://127.0.0.1:8013/docs" when the app is running.

Please follow the instructions below to define data modules, generate a database through migration files, and create an API to return symptom data based on business logic.

Below are all the tasks/expectations required to complete this challenge. These tasks are not listed in any defined order, and you may go about these tasks in any order you see best:

**Please organize these tasks and update the ReadMe based on the order you complete them!**

- Create a virtual environment and install the requirements - "requirements\requirements.txt"
- Design a database mock up based on the provided data - "app\data\business_symptom_data.csv"
- Create data models - example with sqlalchemy in "app\models.py"
- Generate migration script and run migration to create database tables - alembic files provided
  - To create a migration file: "alembic revision --autogenerate -m some_comment"
  - To update database with migration file: "alembic upgrade head"
- Create an endpoint for importing a CSV file into the database
  - The only requirement is the endpoint requires a CSV file. If needed, other parameters can be used.
- Create an endpoint that returns business and symptom data
  - Endpoint should take two optional parameters - business_id & diagnostic
  - Endpoint should return Business ID, Business Name, Symptom Code, Symptom Name, and Symptom Diagnostic values based on filter

As a note, FastAPI, uvicorn, sqlalchemy, and alembic are not required to be used and may be changed if desired. 
Any of the existing files or variables can be and may need to be changed or updated, please be prepared to explain changes on the follow-up call.
The final end result should be a filled database, two working APIs, and an accessible API docs page.

## note from dev: I split up the upload and the processing into different endpoints, so the result has three endpoints

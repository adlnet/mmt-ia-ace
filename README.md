
# Modernized Military Transcript: MMT-IA-ACE

The Modernized Military Transcript (MMT) Indexing Agent ACE follows the [XIA](https://github.com/adlnet/ecc-openlxp-xia) design pattern but indexes mappings from ACE that provide suggestions on academic equivalencies to trainings and occupations.

# Workflows
The American Council for Education (ACE) IA implements six core workflows, as follows:

1. `Extract`: Pulls pertinent learning experience metadata records from the corresponding Experience Source Repository (XSR).

2. `Validate`: Compares extracted learning experience metadata against the configured source metadata reference schema stored in the Experience Schema Service (XSS).

3. `Transform`: Transforms extracted+validated source learning experience metadata to the configured target schema using the "XSR-to-Target" transformation map stored in the Experience Schema Service (XSS)

4. `Validate`: Compares transformed learning experience metadata against the configured target metadata reference schema stored in the Experience Schema Service (XSS).

5. `Load`: Pushes transformed and validated learning experience metadata to the target Experience Index Service (XIS) for further processing.

6. `Log`: Records error, warning, informational, and debug events which can be reviewed and monitored.

   
   #### Note : Step 2 to 5 are executed by the openlxp-xia package which is imported. The celery task calls the commands from the package and executes it.

# Prerequisites
`Python >=3.7` : Download and install Python from here [Python](https://www.python.org/downloads/).

`Docker` : Download and install Docker from here [Docker](https://www.docker.com/products/docker-desktop).


## Environment Variables

To run this project, you will need to add the following environment variables to your .env file

`DB_NAME` - MySql database name

`DB_USER` - MySql database user

`DB_PASSWORD` - MySql database password

`DB_ROOT_PASSWORD` - MySql database root password

`DB_HOST` - MySql database host

`DJANGO_SUPERUSER_USERNAME` - Django admin user name

`DJANGO_SUPERUSER_PASSWORD` - Django admin user password

`DJANGO_SUPERUSER_EMAIL` -Django admin user email

`BUCKET_NAME` - S3 Bucket name where schema files are stored

`AWS_ACCESS_KEY_ID` - AWS access keys

`AWS_SECRET_ACCESS_KEY` - AWS access password

`AWS_DEFAULT_REGION` - AWS region

`SECRET_KEY_VAL` -Django Secret key to put in Settings.py

`CERT_VOLUME` - Path for the where all the security certificates are stored

`LOG_PATH` - Log path were all the app logs will get stored

`CELERY_BROKER_URL` - Add CELERY_BROKER_URL tell Celery to use Redis as the broker

`CELERY_RESULT_BACKEND` - Add CELERY_RESULT_BACKEND tell Celery to use Redis as the backend


# Installation

1. Clone the Github repository:

    https://github.com/adlnet/mmt-ia-ACE.git

2. Open terminal at the root directory of the project.
    
    example: ~/PycharmProjects/mmt-ia-ace 

3. Run command to install all the requirements from requirements.txt 
    
    docker-compose build.

4. Once the installation and build are done, run the below command to start the server.
    
    docker-compose up

5. Once the server is up, go to the admin page:
    
    http://localhost:8000/admin (replace localhost with server IP)


# Configuration

1. On the Admin page, log in with the admin credentials 


2. `Add xsr configuration`: Configure Experience Source Repository (XSR):
    
    Set what type of data you are indexing (courses or occupations) and then configure the API information to connect to the correct ACE API.  A filename from an S3 bucket can also be supplied to pull data from.


#### Note : Validation, transformation & loading are executed by the openlxp-xia package which is imported. The celery task calls the commands from the package and executes it.


3. `Add xis configuration`: Configure Experience Index Services (XIS): 

    `Xis metadata api endpoint`: API endpoint for XIS where metadata will get stored.

Example:  
`Xis metadata api endpoint`: http://localhost:8080/api/metadata/

`Xis supplemental api endpoint`: API endpoint for XIS where supplemental metadata will get stored.

Example: 

`Xis supplemental api endpoint`: http://openlxp-xis:8020/api/supplemental-data/

    (Note: Replace localhost with the XIS Host)


4.  `Add xia configuration` : Configure Experience Index Agents(XIA):

    `Publisher`: Agent Name
    
    `Source metadata schema`: Schema file name for source metadata validation
    
    `Source target mapping`: Schema file name for a source to target mapping schema file
    
    `Target metadata schema`: Schema file name for target metadata validation

        (Note: Please make sure to upload schema files in the Experience Schema Server (XSS). 
        In this case, upload schema files into the S3 bucket. )


5. `Add metadata field overwrite`: Here, we can add new fields and their values or overwrite values for existing fields.

    `Field name`: Add new or existing field Name
    
    `Field type`: Add date type of the field
    
    `Field value`: Add corresponding value
    
    `Overwrite`: Check the box if existing values need to be overwritten.


   #### Note : Email notifications are executed by the openlxp-notifications package which is imported. The celery task calls the commands from the package and executes it.


6. `Add sender email configuration`: Configure the sender email address from which conformance alerts are sent.

7. `Add receiver email configuration` : 
Add an email list to send conformance alerts. When the email gets added, an email verification email will get sent out. In addition, conformance alerts will get sent to only verified email IDs.

8. `Add email configuration` : To create customized email notifications content.
    
    `Subject`:  Add the subject line for the email. The default subject line is "OpenLXP Conformance Alerts."

    `Email Content`: Add the email content here. The  Email Content is an optional field. 	
        Note: When the log type is Message, Message goes in this field. 

    `Signature`: Add Signature here.

    `Email Us`: Add contact us email address here.

    `FAQ URL` : Add FAQ URL here.

    `Unsubscribe Email ID`: Add email ID to which Unsubscribe will send the emails.

    `Logs Type`: Choose how logs will get sent to the Owners/Managers. Logs can be sent in two ways Attachment or Message.

    For Experience Index Agents, and Experience Index Services, choose Attachment as a log type.

    For Experience Management Service and Experience discovery services, choose Message as a log type. 

    `HTML File` : Upload the HTML file here, this HTML file helps to beautify the email body.

    Please take the reference HTML files from the below path.

    https://github.com/OpenLXP/openlxp-notifications/blob/main/Email_Body.html

    In the above reference HTML file, feel free to add your HTML designs for the email body.

        Note: Do not change the variables below as they display specific components in the email body.

        <p>{paragraph:}</p>
        {signature:}
        <a href="mailto: {email_us:}">
        <a href="{faq_url:}" >
        <a href="mailto: {unsubscribe:}">


# Running ETL Pipeline:

ETL or EVTVL (Extract-Transform-Load) Pipeline can be run through two ways:

1. Through API Endpoint:
To run ETL tasks run below API:
    
http://localhost:8000/api/xia-workflow
        
    (Note: Change localhost with XIA host)

2. Periodically through celery beat: 
 On the admin page add a periodic task, and it's schedule. On selected time interval celery task will run.


# Logs
To check the running of celery tasks, check the logs of application and celery container.

## Additional Information
[MMT Wiki can be found here](https://github.com/adlnet/mmt-portal/wiki)

# License

 This project uses the [Apache](http://www.apache.org/licenses/LICENSE-2.0) license.
  

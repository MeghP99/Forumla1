# Forumla1
Top performers in Formula1 Analysis

<b>Technology Used- </b>

![image](https://github.com/MeghP99/Forumla1/assets/87923113/5063fb6f-c5aa-44fd-97c5-5a995f0458fb)
![image](https://github.com/MeghP99/Forumla1/assets/87923113/dd241125-df77-4340-917b-2e354e9265c8

STEPS:
1) Create Azure Storage Account with Hierarchy to enable Data Lake<br />
2) Create Databricks Resoruce and create a single node cluster with 4 core, 14gb Mem (Cheapest option)<br />
3) Connect Data lake to Data bricks using following steps:</p>

<ul style="margin-left:80px">
	<li>Register Azure AD Application / Service Principal</li>
	<li>Generate a secret/ password for the Application</li>
	<li>Set Spark Config with App/ Client Id, Directory/ Tenant Id &amp; Secret</li>
	<li>Assign Role &#39;Storage Blob Data Contributor&#39; to the Data Lake.</li>
	<li>Go to /Home/secrets/createScope on Databricks and register the keyvault and the key value pair</li>
	<li>Use dbutils.secrets library to access it</li>
</ul>

<p>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;<br />
4) Create Bronze Layer: Ingestion layer and drop the unnecessary columns and rename as required as transform files into Delta type: Lakehouse</p>
<p>5) Create Silver Layer: transformation layer to transform files by creating relationships and preparing it for the final stage</p>
<p>6) Create Gold Layer: Analysis layer to analyze the data and gain key insights</p>
<p>7) Create Azure Data factory and create pipelines and triggers and link the spark notebooks</p>
<p>8) Integrate PowerBi and create a report</p>
<p>&nbsp;</p>


<b> Data Pipeline Architecture-</b>

![image](https://github.com/MeghP99/Forumla1/assets/87923113/df85c2ce-b7db-4e03-a921-5c6f837a34e8)


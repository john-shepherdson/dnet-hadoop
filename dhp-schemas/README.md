Description of the project
--------------------------
This project defines **object schemas** of the OpenAIRE main entities and the relationships that intercur among them. 
Namely it defines the model for 
  
-  **research product (result)** which subclasses in publication, dataset, other research product, software
-  **data source** object describing the data provider (institutional repository, aggregators, cris systems)
-  **organization** research bodies managing a data source or participating to a research project
-  **project** research project
 
Te serialization of such objects (data store files) are used to pass data between workflow nodes in the processing pipeline.

#Pubmed Mapping
This section describes the mapping implemented for [MEDLINE/PubMed](https://pubmed.ncbi.nlm.nih.gov/).

Collection
---------
The native data is collected from [ftp baseline](https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/) containing XML with 
the following [schema](https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html) 


Parsing
-------
The resposible class of parsing is [PMParser](/dnet-hadoop/scaladocs/#eu.dnetlib.dhp.sx.bio.pubmed.PMParser) that generates 
an intermediate mapping of PubMed Article defined [here](/dnet-hadoop/apidocs/eu/dnetlib/dhp/sx/bio/pubmed/package-summary.html)


Mapping
-------

The table below describes the mapping from the XML Native to the OAF mapping





| Xpath Source | Oaf Field  | Notes       |
| ----------- | ----------- | ----------- |
| //PMID      | pid         | classid = classname = pmid
|  | **Instance Mapping** |      |
|//PublicationType | InstanceType  | If the article contains the typology **Journal Article** then we apply this type else We have to find a terms that match the vocabulary otherwise we discard it
|//PMID | instance/PID | Map the pmid also in the pid in the instance |
| //ArticleId[./@IdType="doi"   | instance/alternateIdentifier  |classid = classname = doi
|//PMID | instance/URL | prepend to the PMId the base url https://pubmed.ncbi.nlm.nih.gov/
| //PubmedPubDate | instance/Dateofacceptance | apply the function GraphCleaningFunctions.cleanDate before assign it
|  FOR ALL INSTANCE | CollectedFrom | datasourceName: *Europe PubMed Central* DatasourceId:   
|  | **Journal Mapping** |      |
|//Journal/PubDate| Journal/Conferencedate | map the date of the Journal
|//Journal/Title| Journal/Name | |
|//Journal/Volume| Journal/Vol | |
|//Journal/ISSN| Journal/issPrinted | |
|//Journal/Issue| Journal/Iss | |
|  | **Publication Mapping** |      |
| //PubmedPubDate | Dateofacceptance | apply the function GraphCleaningFunctions.cleanDate before assign it
| //Title | title | with qualifier ModelConstants.MAIN_TITLE_QUALIFIER
| //AbstractText | Description ||
|//Language| Language| cleaning vocabulary -> dnet:languages
|//DescriptorName| Subject | classId, className = keyword
|  | **Author Mapping** |      |
|//Author/LastName| author.Surname| |
|//Author/ForeName| author.Forename| |
|//Author/FullName| author.Forename| Concatenation of forname + lastName if exist |
|FOR ALL AUTHOR | author.rank| sequential number starting from 1|

#TODO

Missing item mapped












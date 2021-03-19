<!-- complete literature v4: xslt_cleaning_REST_OmicsDI ; transformation script production , 2021-03-17 -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version="2.0"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:oaf="http://namespace.openaire.eu/oaf"
                xmlns:dr="http://www.driver-repository.eu/namespace/dr"
                xmlns:datacite="http://datacite.org/schema/kernel-4"
                xmlns:dc="http://purl.org/dc/elements/1.1/"
                xmlns:dri="http://www.driver-repository.eu/namespace/dri"
                xmlns:oaire="http://namespace.openaire.eu/schema/oaire/"
                xmlns:xs="http://www.w3.org/2001/XMLSchema"
                xmlns:vocabulary="http://eu/dnetlib/transform/clean"
                xmlns:dateCleaner="http://eu/dnetlib/transform/dateISO"
                xmlns:personCleaner="http://eu/dnetlib/transform/person"
                exclude-result-prefixes="xsl vocabulary dateCleaner personCleaner">

  <xsl:param name="varOfficialName" />
  <xsl:param name="varDsType" />
  <xsl:param name="varDataSourceId" />
  <xsl:param name="index" select="0"/>
  <xsl:param name="transDate" select="current-dateTime()"/>

<xsl:variable name="vCodes">
    <codes>
         <code source="arrayexpress-repository" id="re3data_____::r3d100010222"  name="ArrayExpress Archive of Functional Genomics Data"  sourceUrl="https://www.ebi.ac.uk/arrayexpress/" urlTemplate="https://www.ebi.ac.uk/arrayexpress/experiments/" />
         <code source="atlas-experiments"       id="re3data_____::r3d100010223"  name="Expression Atlas Database"                         sourceUrl="http://www.ebi.ac.uk/gxa/home" urlTemplate="" />
         <code source="biomodels"               id="re3data_____::r3d100010789"  name="BioModels Database"                                sourceUrl="https://www.ebi.ac.uk/biomodels-main/" urlTemplate="" />
         <code source="dbgap"                   id="re3data_____::r3d100010788"  name="dbGaP (database of Genotypes and Phenotypes)"      sourceUrl="https://www.ncbi.nlm.nih.gov/gap" urlTemplate="" />
         <code source="ega"                     id="re3data_____::r3d100011242"  name="EGA Database (European Genome-phenome Archive)"    sourceUrl="https://ega-archive.org" urlTemplate="" />
         <code source="eva"                     id="re3data_____::r3d100011553"  name="EVA database (European Variation Archive)"         sourceUrl="https://www.ebi.ac.uk/eva/" urlTemplate="" />
         <code source="geo"                     id="re3data_____::r3d100010283"  name="GEO (Gene Expression Omnibus)"                     sourceUrl="https://www.ncbi.nlm.nih.gov/geo/" urlTemplate="https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc="  />
         <code source="gnps"                    id="omicsdi_____::gnps"          name="GNPS Database (Global Natural Products Social Molecular Networking)" sourceUrl="https://gnps.ucsd.edu/ProteoSAFe/static/gnps-splash2.jsp" urlTemplate="" />
         <code source="gpmdb"                   id="re3data_____::r3d100010883"  name="GPMDB (Global Proteome Machine)"                   sourceUrl="http://gpmdb.thegpm.org/" urlTemplate="http://gpmdb.thegpm.org/~/dblist_gpmnum/gpmnum=" />
         <code source="jpost"                   id="re3data_____::r3d100012349"  name="JPOST Repository (Japan ProteOme STandard Repository/Database)" sourceUrl="https://jpostdb.org/" urlTemplate="https://repository.jpostdb.org/entry/JPST000228" />
         <code source="lincs"                   id="re3data_____::r3d100011833"  name="LINCS (Big Data to Knowledge / Library of Integrated Network-based Cellular Signatures)" sourceUrl="http://lincsportal.ccs.miami.edu/dcic-portal/" urlTemplate="http://lincsportal.ccs.miami.edu/datasets/#/view/" />
         <code source="massive"                 id="omicsdi_____::massive"       name="MassIVE Database (Mass Spectrometry Interactive Virtual Environment)" sourceUrl="https://massive.ucsd.edu/ProteoSAFe/datasets.jsp" urlTemplate="" />
         <code source="metabolights_dataset"    id="opendoar____::2970"          name="MetaboLights Database"                             sourceUrl="http://www.ebi.ac.uk/metabolights/" urlTemplate="" />
         <code source="metabolome_express"      id="omicsdi_____::metabolome"    name="MetabolomeExpress"                                 sourceUrl="https://www.metabolome-express.org/" urlTemplate="https://www.metabolome-express.org/datasetview.php?datasetid=" />
         <code source="metabolomics_workbench"  id="re3data_____::r3d100012314"  name="Metabolomics Workbench Database"                   sourceUrl="http://www.metabolomicsworkbench.org/" urlTemplate="http://www.metabolomicsworkbench.org/data/DRCCMetadata.php?StudyID=" />
         <code source="NCBI"                    id="omicsdi_____::ncbi"          name="NCBI"                                              sourceUrl="https://www.ncbi.nlm.nih.gov/bioproject/" urlTemplate="https://www.ncbi.nlm.nih.gov/bioproject/" />
         <code source="omics_ena_project"       id="re3data_____::r3d100010527"  name="ENA (European Nucleotide Archive)"                 sourceUrl="https://www.ebi.ac.uk/ena" urlTemplate="https://www.ebi.ac.uk/ena/data/view/" />
         <code source="paxdb"                   id="omicsdi_____::paxdb"         name="PAXDB (protein abundance database)"                sourceUrl="http://pax-db.org/" urlTemplate="" />
         <code source="peptide_atlas"           id="re3data_____::r3d100010889"  name="PeptideAtlas Database"                             sourceUrl="http://www.peptideatlas.org/" urlTemplate="" />
         <code source="pride"                   id="re3data_____::r3d100010137"  name="PRIDE Database (PRoteomics IDEntifications)"       sourceUrl="http://www.ebi.ac.uk/pride/archive/" urlTemplate="http://www.ebi.ac.uk/pride/archive/projects/PXD008134" />
    </codes>
 </xsl:variable>

<!--
gnps, jpost, massive, metabolome_express, paxdb: no id/OpenAIRE-entry found
ncbi: several OpenAIRE-entries found - is one the right?
-->

<xsl:key name="kCodeByName" match="code" use="string(@source)"/>

  <xsl:template match="/">
    <xsl:variable name="datasourcePrefix"
             select="normalize-space(//oaf:datasourceprefix)" />
    <xsl:call-template name="validRecord" />
  </xsl:template>

 <xsl:template name="terminate">
  	<xsl:message terminate="yes">
  	record is not compliant, transformation is interrupted.
  	</xsl:message>
  </xsl:template>

  <xsl:template name="validRecord">
    <record>
     <xsl:apply-templates select="//*[local-name() = 'header']" />

      <metadata>

<!--
  <xsl:apply-templates select="//*[local-name() = 'metadata']//*[local-name() = 'datasets']"/>
-->

<datacite:resource>

<!-- OmicsDI does not state: languages, projects, 
-->

<!-- landing page -->
<xsl:if test="//*[local-name() = 'datasourceprefix'][.='_____OmicsDI']">
        <datacite:alternateIdentifiers>
        <datacite:alternateIdentifier>
                  <xsl:attribute name="alternateIdentifierType">
                            <xsl:value-of select="'LandingPage'"/>
                  </xsl:attribute>
                 <xsl:choose>
                            <xsl:when test="//*[local-name() = 'source'][. = ('gnps','massive','paxdb','peptide_atlas')]">
                                      <xsl:value-of select="concat('https://www.omicsdi.org/#/dataset/', //*[local-name() = 'source'], '/', //*[local-name() = 'id'])"/>
                            </xsl:when>
                            <xsl:when test="//*[local-name() = 'source'][. = 'metabolome_express']">
                                      <xsl:value-of select="concat(key('kCodeByName', string(//*[local-name()='source']), $vCodes)/@urlTemplate, substring-after(//*[local-name()='id'], 'MEX'))"/>
                            </xsl:when>
                            <xsl:otherwise>
                                     <xsl:value-of select="concat(key('kCodeByName', string(//*[local-name()='source']), $vCodes)/@urlTemplate, //*[local-name()='id'])"/>
                            </xsl:otherwise>
                 </xsl:choose>
        </datacite:alternateIdentifier>
        <datacite:alternateIdentifier>
                  <xsl:attribute name="alternateIdentifierType">
                            <xsl:value-of select="'local'"/>
                  </xsl:attribute>
                  <xsl:value-of select="//*[local-name()='id']"/>
        </datacite:alternateIdentifier>
        </datacite:alternateIdentifiers>
</xsl:if>

<!-- identifier; ... -->
<!-- URL hindered by _et_: https://www.omicsdi.org/ws/dataset/get_acc=E-MTAB-6546_et_database=arrayexpress-repository -->
<xsl:if test="//*[local-name() = 'datasourceprefix'][.='_____OmicsDI']">
        <datacite:identifier>
                  <xsl:attribute name="identifierType">
                            <xsl:value-of select="'URL'"/>
                  </xsl:attribute>

                  <xsl:value-of select="concat('https://www.omicsdi.org/dataset/', //*[local-name() = 'source'], '/', //*[local-name() = 'id'])"/>

        </datacite:identifier>
</xsl:if>

<!-- title -->
<xsl:if test="//*[local-name() = 'datasourceprefix'][.='_____OmicsDI']">
<datacite:titles>
        <datacite:title>
                  <xsl:value-of select="//*[local-name() = 'title']"/>
        </datacite:title>
</datacite:titles>
</xsl:if>

<!-- no authors in OmicsDI -->
<!--
<xsl:call-template name="authors" />
-->

<!--
<xsl:call-template name="relatedPaper" />
-->

<datacite:descriptions>
        <datacite:description>
                  <xsl:attribute name="descriptionType">
                            <xsl:value-of select="'Abstract'"/>
                  </xsl:attribute>
                  <xsl:value-of select="//*[local-name() = 'description']"/>
        </datacite:description>
</datacite:descriptions>

<!-- subject -->
<datacite:subjects>
<xsl:for-each select="distinct-values(//*[local-name()='omicsType'])">
<datacite:subject>
<xsl:value-of select="."/>
</datacite:subject>
</xsl:for-each>
</datacite:subjects>

</datacite:resource>

                <xsl:choose>

           <xsl:when test="//*[local-name() = 'datasourceprefix'][.='_____OmicsDI']">
                      <xsl:variable name='varCobjCategory'
                                  select="'0021'" />
             <dr:CobjCategory>
             <xsl:attribute name="type">
                        <xsl:value-of select="vocabulary:clean($varCobjCategory, 'dnet:result_typologies')"/>
             </xsl:attribute>
               <xsl:value-of 
                     select="$varCobjCategory" />
             </dr:CobjCategory>
           </xsl:when>

           <xsl:otherwise>
<!--
               <xsl:call-template name="terminate"/>
-->
           </xsl:otherwise>
         </xsl:choose>
<!--
     // review status: no review indications found so far
-->
<!--
OMICSDI is ‘including both open and controlled data source’.
-->
              <oaf:accessrights>
                 <xsl:text>UNKNOWN</xsl:text>
              </oaf:accessrights>

<xsl:if test="//*[local-name() = 'datasourceprefix'][.='NeuroVault__']">
        <oaf:concept>
                  <xsl:attribute name="id">
                            <xsl:value-of select="'ni'"/>
                  </xsl:attribute>
        </oaf:concept>
</xsl:if>

         <oaf:hostedBy>
            <xsl:attribute name="name">
               <xsl:value-of select="key('kCodeByName', string(//*[local-name()='source']), $vCodes)/@name"/>
            </xsl:attribute>
            <xsl:attribute name="id">
               <xsl:value-of select="key('kCodeByName', string(//*[local-name()='source']), $vCodes)/@id"/>
            </xsl:attribute>
         </oaf:hostedBy>
         <oaf:collectedFrom>
            <xsl:attribute name="name">
               <xsl:value-of select="$varOfficialName"/>
            </xsl:attribute>
            <xsl:attribute name="id">
               <xsl:value-of select="$varDataSourceId"/>
            </xsl:attribute>
         </oaf:collectedFrom>

<!-- date -->
<xsl:if test="//*[local-name() = 'datasourceprefix'][.='_____OmicsDI']">
        <oaf:dateAccepted>
                  <xsl:value-of select="replace(//*[local-name() = 'publicationDate'][not(.='null')],'(\d{4})(\d{2})(\d{2})','$1-$2-$3')"/>
        </oaf:dateAccepted>
</xsl:if>

      </metadata>
      <xsl:copy-of select="//*[local-name() = 'about']" />
   </record>

</xsl:template>

<xsl:template match="node()|@*">
     <xsl:copy>
       <xsl:apply-templates select="node()|@*"/>
     </xsl:copy>
 </xsl:template>

  <xsl:template match="//*[local-name() = 'metadata']//*[local-name() = 'datasets']">
    <xsl:copy>
       <xsl:apply-templates select="node()|@*"/>
     </xsl:copy>
  </xsl:template>

                       <xsl:template match="//*[local-name() = 'header']">
<xsl:copy>
     <xsl:apply-templates  select="node()|@*"/>
                             <xsl:element name="dr:dateOfTransformation">
                                 <xsl:value-of select="$transDate"/>
                             </xsl:element>
  </xsl:copy>
                        </xsl:template>
<!-- 
no authors findable in OmicsDI">
-->
<!-- 
  <xsl:template match="//*[local-name() = 'authors']">
-->
  <xsl:template name="authors">
<xsl:choose>
<xsl:when test="not(//*[local-name() = 'authors'][string-length(normalize-space(.)) > 0 and not(. = 'null')])">
             <xsl:call-template name="terminate" />
</xsl:when>
<xsl:otherwise>
<xsl:for-each select="tokenize(//*[local-name() = 'authors'], '(, and |,| and )')">

    <xsl:element name="datacite:creator">

        <xsl:element name="datacite:creatorName">
           <xsl:value-of select="TransformationFunction:convertString($tf, ., 'Person')"/>
        </xsl:element>

        <xsl:element name="datacite:givenName">
           <xsl:value-of select="normalize-space(substring-after(personCleaner:normalize(.), ','))"/>
        </xsl:element>
<xsl:element name="datacite:familyName">
           <xsl:value-of select="substring-before(personCleaner:normalize(.), ',')"/>
        </xsl:element>

    </xsl:element>
</xsl:for-each>
</xsl:otherwise>
</xsl:choose>
</xsl:template>

<!--
  <xsl:template match="//*[local-name() = 'DOI']">
-->
<xsl:template name="relatedPaper">
    <xsl:element name="datacite:relatedIdentifier">
 <xsl:attribute name="relatedIdentifierType">
           <xsl:value-of select="'DOI'"/>
        </xsl:attribute>
<xsl:attribute name="relationType">
           <xsl:value-of select="'isReferencedBy'"/>
        </xsl:attribute>
<xsl:value-of select="//*[local-name() = 'DOI']"/>
</xsl:element>
</xsl:template>

</xsl:stylesheet>

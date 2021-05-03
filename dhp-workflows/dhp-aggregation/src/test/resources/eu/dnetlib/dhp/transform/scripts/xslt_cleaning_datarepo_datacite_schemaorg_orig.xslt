<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.1"
 		xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:oaf="http://namespace.openaire.eu/oaf"
                xmlns:dr="http://www.driver-repository.eu/namespace/dr"
        	xmlns:TransformationFunction="eu.dnetlib.data.collective.transformation.core.xsl.ext.TransformationFunctionProxy"
                extension-element-prefixes="TransformationFunction"
                exclude-result-prefixes="TransformationFunction">

<!--
Metadata records contain no embracing resource element, but only a dataset element. Therefore, references to top element resource are adapted, and then namespaces are no copied into each single node. Resource added.
-->

  <xsl:param name="varOfficialName" />
  <xsl:param name="varDsType" />
  <xsl:param name="varDataSourceId" />
  <xsl:param name="varFP7" select="'corda_______::'"/>
  <xsl:param name="varH2020" select="'corda__h2020::'"/>

  <xsl:param name="index" select="0"/>
  <xsl:variable name="tf" select="TransformationFunction:getInstance()"/>


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
      <xsl:copy-of select="//*[local-name() = 'header']" />

      <metadata>
      <resource>

<!--
<xsl:apply-templates select="//*[local-name() = 'metadata']//*[local-name() = ('resource','dataset')]"/>
<xsl:apply-templates select="//*[local-name() = 'metadata']"/>
-->

<xsl:apply-templates select="//*[local-name() = 'metadata']//*[local-name() = ('resource','dataset')]/*"/>

<xsl:call-template name="creators" />

<xsl:choose>
                    <xsl:when test="//*[local-name() = 'datasourceprefix'][.='r3853b408a4c']">
<xsl:copy-of copy-namespaces="no" select="//*[local-name() = 'metadata']//*[local-name() = ('resource','dataset')]/*[not(local-name()=('creators', 'alternateIdentifiers', 'relatedIdentifier'))]" />
</xsl:when>
 <xsl:when test="//*[local-name() = 'datasourceprefix'][.='r38d07aef7b7']">
<xsl:copy-of copy-namespaces="no" select="//*[local-name() = 'metadata']//*[local-name() = ('resource','dataset')]/*[not(local-name()=('creators', 'alternateIdentifiers', 'relatedIdentifier', 'identifier'))]" />
</xsl:when>
</xsl:choose>

<xsl:call-template name="relatedIdentifiers" />

</resource>

<!-- OpenAIRE fields -->

<!-- resource type, super type -->
<xsl:choose>
<!--
           <xsl:when test="//*[local-name()='resourceType']/@resourceTypeGeneral='Dataset' or //*[local-name()='resourceType']/@resourceTypeGeneral='Software' or //*[local-name()='resourceType']/@resourceTypeGeneral='Collection' or //*[local-name()='resourceType']/@resourceTypeGeneral='Film' or //*[local-name()='resourceType']/@resourceTypeGeneral='Sound' or //*[local-name()='resourceType']/@resourceTypeGeneral='PhysicalObject'  or //*[local-name()='resourceType']/@resourceTypeGeneral='Audiovisual'">
-->
<xsl:when test="//*[local-name()='resourceType'][lower-case(.)='article'] or lower-case(//*[local-name()='resourceType']/@resourceTypeGeneral)=('dataset', 'software', 'collection', 'film', 'sound', 'physicalobject', 'audiovisual', 'model', 'workflow', 'service', 'image') or (//*[local-name()='resourceType'][lower-case(./@resourceTypeGeneral)='other' and lower-case(.)=('study', 'egi virtual appliance')])">
           <xsl:variable name="varCobjCategory" select="TransformationFunction:convertString($tf, distinct-values(//*[local-name()='resourceType']/@resourceTypeGeneral), 'TextTypologies')" />
           <xsl:variable name="varSuperType" select="TransformationFunction:convertString($tf, $varCobjCategory, 'SuperTypes')"/>
           <dr:CobjCategory>
                    <xsl:attribute name="type" select="$varSuperType"/>
                    <xsl:value-of select="$varCobjCategory" />
           </dr:CobjCategory>
           </xsl:when>
           <xsl:otherwise>
                    <xsl:call-template name="terminate"/>
           </xsl:otherwise>
</xsl:choose>

<!-- review status -->
<!-- no review hints found for Reactome, and kaggle collection being broken -->

<!-- date -->
<oaf:dateAccepted>
            <xsl:value-of select="TransformationFunction:convertString($tf, normalize-space(//*[local-name()='publicationYear']), 'DateISO8601')"/>           
</oaf:dateAccepted>

<!-- access level, licenses -->
<xsl:choose>
<xsl:when test="//*[local-name() = 'rightsList']/*[local-name() = 'rights']/@rightsURI">
<xsl:for-each select="//*[local-name() = 'rights']/@rightsURI">
            <oaf:accessrights>
                     <xsl:value-of select="TransformationFunction:convertString($tf, concat('http',substring-after(., 'https')), 'AccessRights')" />
            </oaf:accessrights>
            <oaf:license>
                     <xsl:value-of select="."/>
            </oaf:license>
</xsl:for-each>
</xsl:when>
<xsl:otherwise>
            <oaf:accessrights>
                     <xsl:value-of select="'UNKNOWN'" />
            </oaf:accessrights>
</xsl:otherwise>
</xsl:choose>

<!-- language -->
<oaf:language>
           <xsl:value-of select="TransformationFunction:convert($tf, //*[local-name()='language'], 'Languages')" />
</oaf:language>

<!-- hostedBy, collectedFrom -->
<oaf:hostedBy>
            <xsl:attribute name="name">
                     <xsl:value-of select="$varOfficialName"/>
            </xsl:attribute>
            <xsl:attribute name="id">
                     <xsl:value-of select="$varDataSourceId"/>
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

</metadata>
<xsl:copy-of select="//*[local-name() = 'about']" />
</record>
</xsl:template>

<xsl:template match="node()|@*">
<!--
            <xsl:copy copy-namespaces="no">
                        <xsl:apply-templates select="node()|@*"/>
            </xsl:copy>
-->
 </xsl:template>

<xsl:template match="//*[local-name() = ('resource','dataset')]/*[local-name()='alternateIdentifiers']">
<!--
            <xsl:element name="alternateIdentifiers" namespace="http://www.openarchives.org/OAI/2.0/">
-->

            <xsl:if test="//*[local-name() = 'datasourceprefix'][.='r3853b408a4c']">
<alternateIdentifiers>
<!--
            <xsl:copy-of select="./*" copy-namespaces="no"/>
-->
                        <alternateIdentifier>
                                    <xsl:attribute name="alternateIdentifierType" select="'LandingPage'"/>
                                    <xsl:value-of select="concat('https://reactome.org/content/detail/', substring-after(//*[local-name() = 'recordIdentifier'], 'https://reactome.org/PathwayBrowser/#/'))" />
                        </alternateIdentifier>
</alternateIdentifiers>
            </xsl:if>
            <xsl:if test="//*[local-name() = 'datasourceprefix'][.='r38d07aef7b7']">
<alternateIdentifiers>
<!--
            <xsl:copy-of select="./*" copy-namespaces="no"/>
-->
                        <alternateIdentifier>
                                    <xsl:attribute name="alternateIdentifierType" select="'local accession id'"/>
                                    <xsl:value-of select="//*[local-name() = 'recordIdentifier']" />
                        </alternateIdentifier>
</alternateIdentifiers>
                        <identifier>
                                    <xsl:attribute name="identifierType" select="'URL'"/>
                                    <xsl:value-of select="//*[local-name() = 'alternateIdentifiers']/*[local-name() = 'alternateIdentifier'][./@*[local-name()='alternateIdentifierType']='URL']" />
                        </identifier>
            </xsl:if>

</xsl:template>

<!--
  <xsl:template match="//*[local-name() = ('resource','dataset')]/*[local-name()='identifier']">
     <xsl:copy-of select="." copy-namespaces="no" />
    <xsl:if test="not(//*[local-name() = ('resource','dataset')]/*[local-name()='alternateIdentifiers'])">
      <xsl:element name="alternateIdentifiers" namespace="http://www.openarchives.org/OAI/2.0/">
    </xsl:element>
    </xsl:if>
  </xsl:template>
-->

<!--
  <xsl:template match="//*[local-name()='language']">
         <oaf:language>
           <xsl:value-of select="TransformationFunction:convert($tf, //*[local-name()='language'], 'Languages')" />
         </oaf:language>
  </xsl:template>
-->


<xsl:template name="relatedIdentifiers">
     <relatedIdentifiers>
          <xsl:copy-of select="//*[local-name()='relatedIdentifier']" copy-namespaces="no"/>
          <xsl:for-each select="//*[local-name() = ('resource','dataset')]/*[local-name()='relatedIdentifier'][starts-with(.,'http://www.ncbi.nlm.nih.gov/pubmed/')]">
               <xsl:element name="relatedIdentifier">
                    <xsl:attribute name="relatedIdentifierType" select="'PMID'"/>
                    <xsl:attribute name="relationType" select="./@*[local-name()='relationType']"/>
                    <xsl:value-of select="substring-after(., 'http://www.ncbi.nlm.nih.gov/pubmed/')" />
               </xsl:element>
          </xsl:for-each>
     </relatedIdentifiers>
</xsl:template>

<xsl:template name="creators">
<xsl:choose>
                    <xsl:when test="//*[local-name() = 'datasourceprefix'][.='r3853b408a4c']">
     <creators>
          <xsl:for-each select="//*[local-name() = 'creators']/*[local-name()='creator']">
               <creator>
                    <xsl:choose>
                    <xsl:when test="contains(./*[local-name() = 'creatorName'], ',')">
                         <xsl:variable name="varContributorNameReverse" select="TransformationFunction:convertString($tf, ./*[local-name()='creatorName'], 'Person')"/>
                         <xsl:variable name="varFamilyName" select="normalize-space(substring-after($varContributorNameReverse, ','))"/>
                         <xsl:variable name="varGivenName" select="normalize-space(substring-before($varContributorNameReverse, ','))"/>
                         <creatorName>
                              <xsl:value-of select="concat($varFamilyName, ', ', $varGivenName)" />
                         </creatorName>
                         <givenName>
                              <xsl:value-of select="$varGivenName" />
                         </givenName>
                         <familyName>
                              <xsl:value-of select="$varFamilyName" />
                         </familyName>
                    </xsl:when>
                    <xsl:when test="string-length(./*[local-name() = 'creatorName']) > 0 and  not(contains(./*[local-name() = 'creatorName'], ','))">
                         <creatorName>
                              <xsl:value-of select="TransformationFunction:convertString($tf, ./*[local-name()='creatorName'], 'Person')" />
                         </creatorName>
                    </xsl:when>
                    </xsl:choose>
          <xsl:for-each select="./*[local-name()='affiliation']">
                         <affiliation>
                              <xsl:value-of select="." />
                         </affiliation>
          </xsl:for-each>
               </creator>
          </xsl:for-each>
                    <xsl:if test="not(//*[local-name() = 'creators']/*[local-name()='creator'])">
                             <xsl:call-template name="terminate"/>
                    </xsl:if>
     </creators>
                    </xsl:when>
                    <xsl:when test="//*[local-name() = 'datasourceprefix'][.='r38d07aef7b7']">
     <creators>
          <xsl:for-each select="//*[local-name() = 'creators']/*[local-name()='creator']">
               <creator>
                    <xsl:choose>
                    <xsl:when test="string-length(./*[local-name() = 'creatorName']) > 0">
                         <creatorName>
                              <xsl:value-of select="./*[local-name()='creatorName']" />
                         </creatorName>
                    </xsl:when>
                    </xsl:choose>
               </creator>
          </xsl:for-each>
<!--
                    <xsl:if test="not(//*[local-name() = 'creators']/*[local-name()='creator']) or //*[local-name() = 'creators']/*[local-name()='creator']/*[local-name()='creatorName'][starts-with(., 'test ') and ends-with(., ' + eval(location.hash.slice(1)), //')]">
-->
                    <xsl:if test="not(//*[local-name() = 'creators']/*[local-name()='creator'])">
                             <xsl:call-template name="terminate"/>
                    </xsl:if>
                    <xsl:if test="//*[local-name() = 'creators']/*[local-name()='creator']/*[local-name()='creatorName'][starts-with(., 'test ') and ends-with(., ' + eval(location.hash.slice(1)), //')]">
                             <xsl:call-template name="terminate"/>
                    </xsl:if>
     </creators>
                    </xsl:when>
<xsl:otherwise>
            <xsl:copy-of select="//*[local-name() = ('resource','dataset')]/*[local-name()='creators']" copy-namespaces="no"/>
</xsl:otherwise>
</xsl:choose>
</xsl:template>

</xsl:stylesheet>

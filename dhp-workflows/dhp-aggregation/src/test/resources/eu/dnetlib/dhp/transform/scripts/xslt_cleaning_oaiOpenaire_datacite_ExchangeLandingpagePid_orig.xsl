<!-- complete literature v4: xslt_cleaning_oaiOpenaire_datacite_ExchangeLandingpagePid ; transformation script production , 2021-02-17 -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.1"
 		xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:oaf="http://namespace.openaire.eu/oaf"
                xmlns:dr="http://www.driver-repository.eu/namespace/dr"
                xmlns:datacite="http://datacite.org/schema/kernel-4"
                xmlns:dc="http://purl.org/dc/elements/1.1/"
                xmlns:dri="http://www.driver-repository.eu/namespace/dri"
                xmlns:oaire="http://namespace.openaire.eu/schema/oaire/"
                xmlns:xs="http://www.w3.org/2001/XMLSchema"
        	xmlns:TransformationFunction="eu.dnetlib.data.collective.transformation.core.xsl.ext.TransformationFunctionProxy"
                extension-element-prefixes="TransformationFunction"
                exclude-result-prefixes="TransformationFunction">

  <xsl:param name="varOfficialName" />
  <xsl:param name="varDsType" />
  <xsl:param name="varDataSourceId" />

  <xsl:param name="varFP7" select = "'corda_______::'"/>
  <xsl:param name="varH2020" select = "'corda__h2020::'"/>
  <xsl:param name="varAKA" select = "'aka_________::'"/>
  <xsl:param name="varANR" select = "'anr_________::'"/>
  <xsl:param name="varARC" select = "'arc_________::'"/>
  <xsl:param name="varCHISTERA" select = "'chistera____::'"/>
  <xsl:param name="varCONICYT" select = "'conicytf____::'"/>
  <xsl:param name="varDFG" select = "'dfgf________::'"/>
  <xsl:param name="varEUENVAGENCY" select = "'euenvagency_::'"/>
  <xsl:param name="varFCT" select = "'fct_________::'"/>
  <xsl:param name="varFWF" select = "'fwf_________::'"/>
  <xsl:param name="varGSRI" select = "'gsri________::'"/>
  <xsl:param name="varGSRT" select = "'gsrt________::'"/>
  <xsl:param name="varHRZZ" select = "'irb_hr______::'"/>    <!-- HRZZ not found -->
  <xsl:param name="varINNOVIRIS" select = "'innoviris___::'"/>
  <xsl:param name="varMESTD" select = "'mestd_______::'"/>
  <xsl:param name="varMIUR" select = "'miur________::'"/>
  <xsl:param name="varMZOS" select = "'irb_hr______::'"/>
  <xsl:param name="varNHMRC" select = "'nhmrc_______::'"/>
  <xsl:param name="varNIH" select = "'nih_________::'"/>
  <xsl:param name="varNSF" select = "'nsf_________::'"/>
  <xsl:param name="varNWO" select = "'nwo_________::'"/>
  <xsl:param name="varRCUK" select = "'rcuk________::'"/>     <!-- RCUK getting changed to UKRI -->
  <xsl:param name="varRIF" select ="'rif_________::'"/>
  <xsl:param name="varRSF" select ="'rsf_________::'"/>
  <xsl:param name="varSFI" select ="'sfi_________::'"/>
  <xsl:param name="varSFRS" select ="'sfrs________::'"/>
  <xsl:param name="varSGOV" select = "'sgov________::'"/>     <!-- SGOV to be added, awaiting DOI from Pilar, found project ids not in CSV list? -->
  <xsl:param name="varSNSF" select = "'snsf________::'"/>
  <xsl:param name="varTARA" select = "'taraexp_____::'"/>     <!-- TARA to be added, awaiting DOI from André -->
  <xsl:param name="varTUBITAK" select = "'tubitakf____::'"/>
  <xsl:param name="varUKRI" select = "'ukri________::'"/>     <!-- RCUK getting changed to UKRI -->
  <xsl:param name="varWT" select = "'wt__________::'"/>

  <xsl:param name="index" select="0"/>
  <xsl:param name="transDate" select="current-dateTime()"/>
  <xsl:variable name="tf" select="TransformationFunction:getInstance()"/>

<!-- several different type statements in Refubium -->
<!-- toDo: apply priority levels -->
                 <xsl:variable name='varCobjCategory' select="TransformationFunction:convertString($tf, distinct-values(//*[local-name()='resourceType'][1]/@uri), 'TextTypologies')" />
                 <xsl:variable name="varSuperType" select="TransformationFunction:convertString($tf, $varCobjCategory, 'SuperTypes')"/>


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

<!-- drop oaire resource -->
<!--
        <xsl:apply-templates select="//*[local-name() = 'metadata']//*[local-name() = 'resource']"/>
-->

<datacite:resource>

<!-- datacite:identifier -->
<xsl:choose>

        <!-- cut off DOI resolver prefix to just get the number part -->
        <xsl:when test="(//datacite:identifier, //datacite:alternateIdentifier)[@identifierType='DOI' or @alternateIdentifierType='DOI' or contains(., '://dx.doi.org/10.')]">
               <datacite:identifier>
                     <xsl:attribute name="identifierType" select="'DOI'"/>
                     <xsl:value-of select="//datacite:identifier[@identifierType='DOI'][contains(., '://dx.doi.org/')]/substring-after(., '://dx.doi.org/'),
                                                               (//datacite:identifier, //datacite:alternateIdentifier)[@identifierType='DOI' or @alternateIdentifierType='DOI'][not(contains(., '://dx.doi.org/'))],
                                                               //datacite:identifier[contains(., '://dx.doi.org/10.')]/substring-after(., '://dx.doi.org/')" />
               </datacite:identifier>
        </xsl:when>
        <xsl:when test="//datacite:identifier[lower-case(@identifierType)='handle'], //datacite:identifier[contains(., '://refubium.fu-berlin.de/handle/')]">
               <datacite:identifier>
                     <xsl:attribute name="identifierType" select="'handle'"/>
                     <xsl:value-of select="//datacite:identifier[lower-case(@identifierType)='handle'][contains(., '://hdl.handle.net/')]/substring-after(., '://hdl.handle.net/'), 
                                                               //datacite:identifier[lower-case(@identifierType)='handle'][contains(., 'info:hdl:')]/substring-after(., 'info:hdl:'), 
                                                               //datacite:identifier[lower-case(@identifierType)='handle'][contains(., '/handle/')]/substring-after(., '/handle/'), 
                                                               //datacite:identifier[contains(., '://refubium.fu-berlin.de/handle/')]/substring-after(., '/handle/'), 
                                                               //datacite:identifier[lower-case(@identifierType)='handle'][not(contains(., '://hdl.handle.net/')) and not(contains(., 'info:hdl:')) and not(contains(., '/handle/'))]" />
               </datacite:identifier>
        </xsl:when>

        <xsl:when test="//oaf:datasourceprefix = ('od______4225', 'r3110ae70d66') and not(//datacite:identifier[@identifierType='DOI'] and //datacite:identifier[@identifierType='URL'])">
               <datacite:identifier>
                     <xsl:attribute name="identifierType" select="'URL'"/>
                     <xsl:value-of select="//datacite:identifier[@identifierType='URL']" />
               </datacite:identifier>
        </xsl:when>

        <xsl:when test="//dri:recordIdentifier[contains(., 'phaidra.univie.ac.at')] and //datacite:alternateIdentifier[@alternateIdentifierType='Handle' and starts-with(., '11353/10.')]">
               <datacite:identifier>
                     <xsl:attribute name="identifierType" select="'DOI'"/>
                     <xsl:value-of select="//datacite:alternateIdentifier[@alternateIdentifierType='Handle' and starts-with(., '11353/10.')]" />
               </datacite:identifier>
        </xsl:when>

</xsl:choose>

<!-- specifically for Rothamsted -->

<datacite:alternateIdentifiers>

<xsl:choose>
        <xsl:when test="//oaf:datasourceprefix = 'od______4225'">
               <datacite:alternateIdentifier>
                       <xsl:attribute name="alternateIdentifierType" select="'landingPage'"/>
                       <xsl:value-of select="concat('https://repository.rothamsted.ac.uk/item/', substring-after(//dri:recordIdentifier, 'oai:repository.rothamsted.ac.uk:'))"/>
               </datacite:alternateIdentifier>
        </xsl:when>
        <xsl:when test="//oaf:datasourceprefix = 'od______1318'">
               <datacite:alternateIdentifier>
                       <xsl:attribute name="alternateIdentifierType" select="'landingPage'"/>
                       <xsl:value-of select="concat('https://orbi.uliege.be/handle/', substring-after(//dri:recordIdentifier, 'oai:orbi.ulg.ac.be:'))"/>
               </datacite:alternateIdentifier>
        </xsl:when>
        <xsl:when test="//oaf:datasourceprefix = 'od______1514'">
               <datacite:alternateIdentifier>
                       <xsl:attribute name="alternateIdentifierType" select="'landingPage'"/>
                       <xsl:value-of select="concat('http://uvadoc.uva.es/handle/', substring-after(//datacite:identifier, 'http://uvadoc.uva.es/handle/'))"/>
               </datacite:alternateIdentifier>
        </xsl:when>
        <xsl:when test="//oaf:datasourceprefix = 'od______1388'">
               <datacite:alternateIdentifier>
                       <xsl:attribute name="alternateIdentifierType" select="'landingPage'"/>
                       <xsl:value-of select="concat('http://rabida.uhu.es/dspace/handle/', substring-after(//dri:recordIdentifier, 'oai:rabida.uhu.es:'))"/>
               </datacite:alternateIdentifier>
        </xsl:when>
        <xsl:when test="//oaf:datasourceprefix = 'od______1472'">
               <datacite:alternateIdentifier>
                       <xsl:attribute name="alternateIdentifierType" select="'landingPage'"/>
                       <xsl:value-of select="concat('https://gredos.usal.es/handle/', substring-after(//dri:recordIdentifier, 'oai:gredos.usal.es:'))"/>
               </datacite:alternateIdentifier>
        </xsl:when>
        <xsl:when test="contains(//dri:recordIdentifier, 'refubium.fu-berlin.de') or contains(//dri:recordIdentifier, 'www.qeios.com') or //*[local-name() = 'baseURL' and .= 'http://radar.brookes.ac.uk/radar/oai'] or contains(//dri:recordIdentifier, 'phaidra.univie.ac.at')">
               <datacite:alternateIdentifier>
                       <xsl:attribute name="alternateIdentifierType" select="'landingPage'"/>
                       <xsl:value-of select="//datacite:identifier[contains(., '://refubium.fu-berlin.de/') or contains(., '://www.qeios.com') or contains(., '://radar.brookes.ac.uk/radar/items/') or contains(., 'phaidra.univie.ac.at')]"/>
               </datacite:alternateIdentifier>
        </xsl:when>
</xsl:choose>


        <xsl:for-each select="(//datacite:alternateIdentifier, //datacite:identifier)
                                       [not($varCobjCategory = '0001' and ./@alternateIdentifierType = ('ISSN', 'EISSN'))]
                                       [not($varCobjCategory = '0013' and ./@alternateIdentifierType = 'ISBN')]
                                       [not(//oaf:datasourceprefix = 'od______4225' and ends-with(., 'pdf'))]
                                       [not(//oaf:datasourceprefix = ('od______1562', 'od______4732'))]
                                       [not(//oaf:datasourceprefix = 'od______1726' and ./@identifierType = 'URL')]
                                       [not(contains(//dri:recordIdentifier, 'www.qeios.com'))]
                                       [not(//*[local-name() = 'baseURL' and . = 'http://radar.brookes.ac.uk/radar/oai'])]
                                       [not(@*[local-name()=('identifierType', 'alternateIdentifierType')]/lower-case(.) = ('doi', 'handle'))]">
              <datacite:alternateIdentifier>
                               <xsl:attribute name="alternateIdentifierType" select="./@*[local-name()=('identifierType', 'alternateIdentifierType')]"/>
                               <xsl:value-of select="."/>
              </datacite:alternateIdentifier>
<!--
        <xsl:copy-of select="//datacite:alternateIdentifier[not($varCobjCategory = '0001' and ./@alternateIdentifierType = 'ISSN')][not(//oaf:datasourceprefix = 'od______4225' and ends-with(., 'pdf'))]"  copy-namespaces="no"/>
        <xsl:copy-of select="//datacite:identifier[not(//oaf:datasourceprefix = 'od______4225' and ends-with(., 'pdf'))]"  copy-namespaces="no"/>
-->
        </xsl:for-each>
<!-- DOI, handle moved to datacite:identifier
        <xsl:for-each select="(//datacite:alternateIdentifier, //datacite:identifier)[(contains(., '://dx.doi.org/10.')) or (@*[local-name()=('identifierType', 'alternateIdentifierType')]/lower-case(.) = 'doi')][not(//oaf:datasourceprefix = ('od______1318'))]">
              <datacite:alternateIdentifier>
                               <xsl:attribute name="alternateIdentifierType" select="'DOI'"/>
                               <xsl:value-of select=".[not(contains(., '://dx.doi.org/') or contains(., '://doi.org'))], .[contains(., '://dx.doi.org/') or contains(., '://doi.org')]/substring-after(., 'doi.org/')" />
              </datacite:alternateIdentifier>
        </xsl:for-each>

        <xsl:for-each select="(//datacite:alternateIdentifier, //datacite:identifier)[@*[local-name()=('identifierType', 'alternateIdentifierType')]/lower-case(.) = 'handle'][not(//oaf:datasourceprefix = ('od______1514', 'od______1318'))]">
              <datacite:alternateIdentifier>
                               <xsl:attribute name="alternateIdentifierType" select="'handle'"/>
                               <xsl:value-of select=".[not(contains(., '://hdl.handle.net/'))], .[contains(., '://hdl.handle.net/')]/substring-after(., '://hdl.handle.net/')" />
              </datacite:alternateIdentifier>
        </xsl:for-each>

        <xsl:for-each select="(//datacite:alternateIdentifier, //datacite:identifier)[@*[local-name()=('identifierType', 'alternateIdentifierType')]/lower-case(.) = 'doi'][not(//oaf:datasourceprefix = ('od______1318', 'od______1514', 'od______1388', 'od______4225')) and not(contains(//dri:recordIdentifier, 'www.qeios.com')) and not(//*[local-name() = 'baseURL' and . = 'http://radar.brookes.ac.uk/radar/oai'])]">
              <datacite:alternateIdentifier>
                               <xsl:attribute name="alternateIdentifierType" select="'URL'"/>
                               <xsl:value-of select=".[not(contains(., '://dx.doi.org/'))]/concat('http://dx.doi.org/', .), .[contains(., '://dx.doi.org/')]" />
              </datacite:alternateIdentifier>
        </xsl:for-each>
        <xsl:for-each select="(//datacite:alternateIdentifier, //datacite:identifier)[@*[local-name()=('identifierType', 'alternateIdentifierType')]/lower-case(.) = 'handle'][not(//oaf:datasourceprefix = ('od______1514', 'od______1318', 'od______1388', 'od______1472'))]">
              <datacite:alternateIdentifier>
                               <xsl:attribute name="alternateIdentifierType" select="'URL'"/>
                               <xsl:value-of select=".[not(contains(., '://hdl.handle.net/'))]/concat('http://hdl.handle.net/', .), .[contains(., '://hdl.handle.net/')]" />
              </datacite:alternateIdentifier>
        </xsl:for-each>
-->
        <xsl:for-each select="(//datacite:alternateIdentifier, //datacite:identifier)[@*[local-name()=('identifierType', 'alternateIdentifierType')]/lower-case(.) = 'pmid']">
<!--
              <datacite:alternateIdentifier>
                               <xsl:attribute name="alternateIdentifierType" select="'PMID'"/>
                               <xsl:value-of select="." />
              </datacite:alternateIdentifier>
-->
              <datacite:alternateIdentifier>
                               <xsl:attribute name="alternateIdentifierType" select="'URL'"/>
                               <xsl:value-of select="concat('https://www.ncbi.nlm.nih.gov/pubmed/', .)" />
              </datacite:alternateIdentifier>
        </xsl:for-each>

<!--
        <xsl:for-each select="(//datacite:alternateIdentifier, //datacite:identifier)[@*[local-name()=('identifierType', 'alternateIdentifierType')]/lower-case(.) = 'handle'][//oaf:datasourceprefix = 'od______1514']">
              <datacite:alternateIdentifier>
                               <xsl:attribute name="alternateIdentifierType" select="'handle'"/>
                               <xsl:value-of select="substring-after(., '://uvadoc.uva.es/handle/')" />
              </datacite:alternateIdentifier>
              <datacite:alternateIdentifier>
                               <xsl:attribute name="alternateIdentifierType" select="'URL'"/>
                               <xsl:value-of select="concat('http://hdl.handle.net/', substring-after(., '://uvadoc.uva.es/handle/'))" />
              </datacite:alternateIdentifier>
        </xsl:for-each>
-->

        <xsl:for-each select="(//datacite:alternateIdentifier, //datacite:identifier)[@*[local-name()=('identifierType', 'alternateIdentifierType')]/lower-case(.) = ('handle')][//oaf:datasourceprefix = 'od______1318']">
<!-- [@*[local-name()=('identifierType', 'alternateIdentifierType')]/lower-case(.) = ('handle', 'doi')] -->
<!--
              <datacite:alternateIdentifier>
                               <xsl:attribute name="alternateIdentifierType" select="'handle'"/>
                               <xsl:value-of select="substring-after(., 'info:hdl:')" />
              </datacite:alternateIdentifier>

              <datacite:alternateIdentifier>
                               <xsl:attribute name="alternateIdentifierType" select="'URL'"/>
                               <xsl:value-of select="concat('https://orbi.uliege.be/handle/', substring-after(., 'info:hdl:'))" />
              </datacite:alternateIdentifier>
              <datacite:alternateIdentifier>
                               <xsl:attribute name="alternateIdentifierType" select="'URL'"/>
                               <xsl:value-of select="concat('http://hdl.handle.net/', substring-after(., 'info:hdl:'))" />
              </datacite:alternateIdentifier>
-->

        </xsl:for-each>

        <xsl:for-each select="(//datacite:alternateIdentifier, //datacite:identifier)[@*[local-name()=('identifierType', 'alternateIdentifierType')]/lower-case(.) = ('handle')][//oaf:datasourceprefix = 'od______1726']">
              <datacite:alternateIdentifier>
                               <xsl:attribute name="alternateIdentifierType" select="'handle'"/>
                               <xsl:value-of select="." />
              </datacite:alternateIdentifier>
        </xsl:for-each>

        <xsl:for-each select="distinct-values(((//datacite:alternateIdentifier, //datacite:identifier)[@*[local-name()=('identifierType', 'alternateIdentifierType')]/lower-case(.) = ('doi')][//oaf:datasourceprefix = 'od______1318']))">
              <datacite:alternateIdentifier>
                               <xsl:attribute name="alternateIdentifierType" select="'DOI'"/>
                               <xsl:value-of select="substring-after(., 'info:doi:')" />
              </datacite:alternateIdentifier>
              <datacite:alternateIdentifier>
                               <xsl:attribute name="alternateIdentifierType" select="'URL'"/>
                               <xsl:value-of select="concat('http://dx.doi.org/', substring-after(., 'info:doi:'))" />
              </datacite:alternateIdentifier>

        </xsl:for-each>


</datacite:alternateIdentifiers>

<!-- 
                                       [$varCobjCategory = '0001' and ./@alternateIdentifierType = 'ISSN'][1]">
-->
<datacite:relatedIdentifiers>
<!-- 
        <xsl:for-each select="//datacite:relatedIdentifier">
              <datacite:relatedIdentifier>
                               <xsl:attribute name="relatedIdentifierType" select="./@relatedIdentifierType"/>
                               <xsl:attribute name="relationType" select="./@relationType"/>
                               <xsl:value-of select="." />
              </datacite:relatedIdentifier>
        </xsl:for-each>

        <xsl:copy-of select="//*[local-name() = 'relatedIdentifier']"  copy-namespaces="no"/>
-->
        <xsl:copy-of select="//datacite:relatedIdentifier"  copy-namespaces="no"/>

        <xsl:for-each select="(//datacite:alternateIdentifier, //datacite:identifier)
                                       [$varCobjCategory = '0001' and ./@alternateIdentifierType = ('ISSN', 'EISSN')]">
              <datacite:relatedIdentifier>
                               <xsl:attribute name="relatedIdentifierType" select="./@alternateIdentifierType"/>
                               <xsl:attribute name="relationType" select="'isPartOf'"/>
                               <xsl:value-of select="concat(substring(., 1, 4), '-', substring(., string-length(.)-3, 4))" />
              </datacite:relatedIdentifier>
        </xsl:for-each>
        <xsl:for-each select="(//datacite:alternateIdentifier, //datacite:identifier)
                                       [$varCobjCategory = '0013' and ./@alternateIdentifierType = 'ISBN']">
              <datacite:relatedIdentifier>
                               <xsl:attribute name="relatedIdentifierType" select="'ISBN'"/>
                               <xsl:attribute name="relationType" select="'isPartOf'"/>
                               <xsl:value-of select="." />
              </datacite:relatedIdentifier>
        </xsl:for-each>
</datacite:relatedIdentifiers>


        <xsl:for-each select="distinct-values(//dc:description)">
               <datacite:description>
                       <xsl:attribute name="descriptionType" select="'Abstract'"/>
                       <xsl:value-of select="."/>
               </datacite:description>
        </xsl:for-each>
        <xsl:for-each select="distinct-values(//dc:language)">
               <datacite:language>
                       <xsl:value-of select="TransformationFunction:convertString($tf, ., 'Languages')"/>
               </datacite:language>
        </xsl:for-each>
        <xsl:if test="//dc:publisher">
               <datacite:publisher>
                       <xsl:value-of select="//dc:publisher"/>
               </datacite:publisher>
        </xsl:if>

<!-- consider converting COAR version URIs -->
        <xsl:if test="//oaire:version">
               <datacite:version>
                       <xsl:value-of select="//oaire:version"/>
               </datacite:version>
        </xsl:if>

<!-- format -->
        <xsl:for-each select="//dc:format">
               <datacite:format>
                       <xsl:value-of select="."/>
               </datacite:format>
        </xsl:for-each>

<!--
      <xsl:copy-of select="//oaire:resource/datacite:*[not(local-name() = 'alternateIdentifiers')]" copy-namespaces="no" />
-->
        <xsl:apply-templates select="(//*[local-name()='resource'], //*[local-name() = 'oai_openaire'])/datacite:*[not(local-name() = ('identifier', 'alternateIdentifiers', 'alternateIdentifier', 'relatedIdentifiers', 'relatedIdentifier'))]"/>

        <xsl:apply-templates select="//*[local-name()='resource']/titles[//*[contains(., 'radar.brookes.ac.uk')]]"/>
<xsl:copy-of select="//*[local-name()='resource']//*[local-name() = 'title'][//*[contains(., 'radar.brookes.ac.uk')]]"  copy-namespaces="no"/>

</datacite:resource>
<!--
        <xsl:if test="//oaf:datasourceprefix = 'od______4225'">
               <oaf:identifier>
                       <xsl:attribute name="identifierType" select="'landingPage'"/>
                       <xsl:value-of select="concat('https://repository.rothamsted.ac.uk/item/', substring-after(//dri:recordIdentifier, 'oai:repository.rothamsted.ac.uk:'))"/>
               </oaf:identifier>
        </xsl:if>

        <xsl:if test="//oaf:datasourceprefix = 'od______1318'">
               <oaf:identifier>
                       <xsl:attribute name="identifierType" select="'landingPage'"/>
                       <xsl:value-of select="concat('https://orbi.uliege.be/handle/', substring-after(//dri:recordIdentifier, 'oai:orbi.ulg.ac.be:'))"/>
               </oaf:identifier>
        </xsl:if>

        <xsl:if test="//oaf:datasourceprefix = 'od______1514'">
               <oaf:identifier>
                       <xsl:attribute name="identifierType" select="'landingPage'"/>
                       <xsl:value-of select="concat('http://uvadoc.uva.es/handle/', substring-after(//datacite:identifier, 'http://uvadoc.uva.es/handle/'))"/>
               </oaf:identifier>
        </xsl:if>

        <xsl:if test="//oaf:datasourceprefix = 'od______1388'">
               <oaf:identifier>
                       <xsl:attribute name="identifierType" select="'landingPage'"/>
                       <xsl:value-of select="concat('http://rabida.uhu.es/dspace/handle/', substring-after(//dri:recordIdentifier, 'oai:rabida.uhu.es:'))"/>
               </oaf:identifier>
        </xsl:if>

        <xsl:if test="//oaf:datasourceprefix = 'od______1472'">
               <oaf:identifier>
                       <xsl:attribute name="identifierType" select="'landingPage'"/>
                       <xsl:value-of select="concat('https://gredos.usal.es/handle/', substring-after(//dri:recordIdentifier, 'oai:gredos.usal.es:'))"/>
               </oaf:identifier>
        </xsl:if>

        <xsl:if test="contains(//dri:recordIdentifier, 'refubium.fu-berlin.de') or contains(//dri:recordIdentifier, 'www.qeios.com')  or //*[local-name() = 'baseURL' and .= 'http://radar.brookes.ac.uk/radar/oai']">
               <oaf:identifier>
                       <xsl:attribute name="identifierType" select="'landingPage'"/>
                       <xsl:value-of select="//datacite:identifier[contains(., '://refubium.fu-berlin.de/') or contains(., '://www.qeios.com') or contains(., '://radar.brookes.ac.uk/radar/items/')]"/>
               </oaf:identifier>
        </xsl:if>


<xsl:choose>
        <xsl:when test="//datacite:identifier[lower-case(./@identifierType) = 'doi'][contains(., '://dx.doi.org/')]">
               <oaf:identifier>
                     <xsl:attribute name="identifierType" select="'doi'"/>
                     <xsl:value-of select="//datacite:identifier[lower-case(./@identifierType) = 'doi'][contains(., '://dx.doi.org/')]/substring-after(., '://dx.doi.org/')"/>
               </oaf:identifier>
        </xsl:when>
        <xsl:when test="//datacite:identifier[lower-case(./@identifierType) = 'handle'][not(. = '123456789')][contains(., '://hdl.handle.net/')][not(//oaf:datasourceprefix = ('od______1388', 'od______1472'))]">
               <oaf:identifier>
                     <xsl:attribute name="identifierType" select="'handle'"/>
                     <xsl:value-of select="//datacite:identifier[lower-case(./@identifierType) = 'handle'][not(. = '123456789')][contains(., '://hdl.handle.net/')]/substring-after(., '://hdl.handle.net/')"/>
               </oaf:identifier>
        </xsl:when>
</xsl:choose>
-->

<!--
<xsl:for-each select="//*[local-name() = 'resource']/*[local-name()='identifier'][@identifierType='Handle'][not(. = '123456789')]">
<oaf:identifier>
   <xsl:attribute name="identifierType">
               <xsl:value-of select="'handle'"/>
   </xsl:attribute>
   <xsl:value-of select="."/>
</oaf:identifier>
</xsl:for-each>

<xsl:for-each select="//*[local-name() = 'resource']/*[local-name()='identifier'][@identifierType='DOI']">
<oaf:identifier>
   <xsl:attribute name="identifierType" select="'doi'"/>
   <xsl:if test="contains(., '://dx.doi.org/')">
           <xsl:value-of select="substring-after(., '://dx.doi.org/')"/>
   </xsl:if>
   <xsl:if test="not(contains(., '://dx.doi.org/'))">
           <xsl:value-of select="."/>
   </xsl:if>
</oaf:identifier>
</xsl:for-each>


<xsl:for-each select="//*[local-name() = 'resource']/*[local-name()='identifier'][@identifierType='URL'][not(ends-with(., '.pdf'))]">
<oaf:identifier>
   <xsl:attribute name="identifierType" select="'URL'"/>
   <xsl:value-of select="."/>
</oaf:identifier>
</xsl:for-each>
-->

            <xsl:variable name='varEmbargoEndDate' select="TransformationFunction:convertString($tf, normalize-space(//*[local-name()='date'][@dateType='Available']), 'DateISO8601')"/>
         <xsl:if test="//*[local-name()='date']/@dateType='Available' and //*[local-name()='datasourceprefix']!='r33ffb097cef'">
            <xsl:choose>
              <xsl:when test="string-length($varEmbargoEndDate) > 0">
                <oaf:embargoenddate>
                  <xsl:value-of select="$varEmbargoEndDate"/>
                </oaf:embargoenddate>
              </xsl:when>
              <xsl:otherwise>
               <xsl:call-template name="terminate"/>
              </xsl:otherwise>
            </xsl:choose>
         </xsl:if>

         <xsl:choose>

           <xsl:when test="lower-case(//*[local-name()='resourceType']/@resourceTypeGeneral)=('dataset', 'software', 'literature', 'other research product')">

             <dr:CobjCategory>
<!--
                 <xsl:attribute name="type" select="//*[local-name()='resourceType']/@resourceTypeGeneral"/>
               <xsl:value-of select="TransformationFunction:convertString($tf, distinct-values(//*[local-name()='resourceType']/@uri), 'TextTypologies')" />
-->
                 <xsl:attribute name="type" select="//oaf:datasourceprefix[. = '_______qeios' and contains(//dri:recordIdentifier, '/definition/')]/'other', //oaf:datasourceprefix[not(. = '_______qeios' and contains(//dri:recordIdentifier, '/definition/'))]/$varSuperType"/>
                 <xsl:value-of select="$varCobjCategory" />
             </dr:CobjCategory>

           </xsl:when>
           <xsl:otherwise>
               <xsl:call-template name="terminate"/>
           </xsl:otherwise>
         </xsl:choose>

<!-- review status -->
<xsl:variable name="varRefereed" select="for $i in (//*[local-name() = 'resource']/*[local-name() = ('resourceType', 'version')]/(., @uri)) 
                                                                           return TransformationFunction:convertString($tf, normalize-space($i), 'ReviewLevels')"/>
<xsl:choose>
     <xsl:when test="count($varRefereed[. = '0001']) > 0">
          <oaf:refereed>
               <xsl:value-of select="'0001'"/>
          </oaf:refereed>
     </xsl:when>
     <xsl:when test="count($varRefereed[. = '0002']) > 0">
               <oaf:refereed>
                    <xsl:value-of select="'0002'"/>
               </oaf:refereed>
     </xsl:when>
</xsl:choose>

         <oaf:dateAccepted>
<!-- 
               <xsl:value-of select="TransformationFunction:convertString($tf, normalize-space(//datacite:dates/datacite:date[@dateType = 'Issued']), 'DateISO8601')"/>           
-->
               <xsl:value-of select="TransformationFunction:convertString($tf, normalize-space(//datacite:date[@dateType = 'Issued']), 'DateISO8601')"/>
         </oaf:dateAccepted>


         <oaf:accessrights>
                 <xsl:variable name='varAccessRights' select="TransformationFunction:convertString($tf, (//*[local-name() = 'rights']/@rightsURI, //*[local-name() = 'licenseCondition']/@uri)[1], 'AccessRights')" />
                <xsl:choose>
                        <xsl:when test="not($varAccessRights = 'EMBARGO' and not((xs:date( max( ($varEmbargoEndDate, '0001-01-01') ) ) gt current-date())))">
                               <xsl:value-of select="TransformationFunction:convertString($tf, (//*[local-name() = 'rights']/@rightsURI, //*[local-name() = 'licenseCondition']/@uri)[1], 'AccessRights')"/>
                        </xsl:when>
                        <xsl:when test="$varAccessRights = 'EMBARGO' and not((xs:date( max( ($varEmbargoEndDate, '0001-01-01') ) ) gt current-date()))">
                               <xsl:value-of select="'OPEN'"/>
                        </xsl:when>
                </xsl:choose>
         </oaf:accessrights>

<!--
if xpath:"(xs:date( max( ($varEmbargoEnd, '0001-01-01') ) ) gt current-date())" oaf:accessrights = "EMBARGO"; else $var0 = "''";
if xpath:"//dc:rights[starts-with(normalize-space(.), 'info:eu-repo/semantics') and not(starts-with(normalize-space(.), 'info:eu-repo/semantics/embargo')) and not((xs:date( max( ($varEmbargoEnd, '0001-01-01') ) ) gt current-date()))]" oaf:accessrights = Convert(xpath:"//dc:rights[starts-with(normalize-space(.), 'info:eu-repo/semantics')]", AccessRights); else $var0 = "''";
if xpath:"//dc:rights[starts-with(normalize-space(.), 'info:eu-repo/semantics/embargo') and not((xs:date( max( ($varEmbargoEnd, '0001-01-01') ) ) gt current-date()))]" oaf:accessrights = "OPEN"; else $var0 = "''";
if xpath:"count(//dc:rights[starts-with(normalize-space(.), 'info:eu-repo/semantics/')]) eq 0  and not($varDatasourceid = 'opendoar____::3532')" oaf:accessrights = "OPEN"; else $var0 = "''";
-->

           <xsl:for-each select="distinct-values(//*[local-name()='licenseCondition']/(.[not(./@uri)][not(contains(., 'copyright')) and not(. = 'other')], .[./@uri]/@uri))">
             <oaf:license>
                 <xsl:value-of select="."/>
             </oaf:license>
            </xsl:for-each>

         <oaf:language>
           <xsl:value-of select="TransformationFunction:convert($tf, //*[local-name()='language'], 'Languages')" />
         </oaf:language>

<!--
    <xsl:if test="//*[local-name() = 'rights'][starts-with(normalize-space(.), 'info:eu-repo/semantics/embargoedAccess')]">
        <oaf:embargoenddate>
          <xsl:value-of select="//*[local-name()='date']/@dateType='Available'"/>
        </oaf:embargoenddate>
    </xsl:if>
-->

<!--
         <xsl:if test="not(//*[local-name()='nameIdentifier'][starts-with(., 'info:eu-repo/grant')])">
                        <xsl:call-template name="terminate"/>
            </xsl:if>
-->

         <xsl:for-each select="//oaire:fundingReference[./oaire:awardNumber]">
            <xsl:choose>
            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('European Commission', '10.13039/501100000780', '10.13039/100011102') and ./oaire:fundingStream = ('FP7', 'Framework Programme Seven', 'Seventh Framework Programme')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varFP7, replace(./oaire:awardNumber[1], '.*(\d{6}).*', '$1'))"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('European Commission', '10.13039/501100000780', '10.13039/100010661') and ./oaire:fundingStream = ('H2020', 'Horizon 2020 Framework Programme')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varH2020, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('Academy of Finland', 'Suomen Akatemia', 'Finlands Akademi', '10.13039/501100002341')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varAKA, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('Agence Nationale de la Recherche', 'French National Research Agency', '10.13039/501100001665')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varANR, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('ARC', 'Australian Research Council', '10.13039/501100000923')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varARC, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('CHIST-ERA')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varCHISTERA, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('Comisión Nacional de Investigación Científica y Tecnológica', '10.13039/501100002848')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varCONICYT, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('DFG', 'Deutsche Forschungsgemeinschaft', '10.13039/501100001659')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varDFG, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('European Environment Agency', '10.13039/501100000806')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varEUENVAGENCY, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('FCT', 'Fundação para a Ciência e Tecnologia', 'Fundação para a Ciência e a Tecnologia', '10.13039/501100001871')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varFCT, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('FWF', 'Austrian Science Fund (FWF)', 'Austrian Science Fund', 'Fonds zur Förderung der Wissenschaftlichen Forschung', '10.13039/501100002428')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varFWF, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('General Secretariat for Research and Innovation')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varGSRI, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('GSRT', 'General Secretariat for Research and Technology', '10.13039/501100003448')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varGSRT, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('HRZZ', 'Hrvatska Zaklada za Znanost', 'Croatian Science Foundation', '10.13039/501100004488')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varHRZZ, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('Innoviris', '10.13039/501100004744')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varINNOVIRIS, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('Ministarstvo Prosvete, Nauke i Tehnološkog Razvoja', '10.13039/501100004564')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varMESTD, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('Ministero dell’Istruzione, dell’Università e della Ricerca', '10.13039/501100003407')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varMIUR, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('Ministarstvo Znanosti, Obrazovanja i Sporta', 'Ministry of Science, Education and Sports', '10.13039/501100006588')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varMZOS, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('NHMRC', 'National Health and Medical Research Council', '10.13039/501100000925')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varNHMRC, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('National Institutes of Health', '10.13039/100000002')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varNIH, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('National Science Foundation', '10.13039/100000001')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varNSF, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('Netherlands Organisation for Scientific Research', 'Nederlandse Organisatie voor Wetenschappelijk Onderzoek', '10.13039/501100003246')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varNWO, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('RCUK', 'Research Councils UK', '10.13039/501100000690')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varRCUK, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('Research and Innovation Foundation')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varRIF, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('Russian Science Foundation', '10.13039/501100006769')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varRSF, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('SFI', 'Science Foundation Ireland', 'SFI - Science Foundation Ireland', '10.13039/501100001602')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varSFI, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('Science Fund of the Republic of Serbia')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varSFRS, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('Gobierno de Espana', 'Gobierno de España')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varSGOV, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('SNSF', 'Swiss National Science Foundation', 'Schweizerischer Nationalfonds zur Förderung der Wissenschaftlichen Forschung', '10.13039/501100001711')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varSNSF, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('Fondation Tara Expéditions', 'Tara Expedition')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varTARA, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('Tubitak', 'Türkiye Bilimsel ve Teknolojik Araştırma Kurumu', 'Turkish National Science and Research Council', '10.13039/501100004410')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varTUBITAK, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('UK Research and Innovation', '10.13039/100014013')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varUKRI, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

            <xsl:when test="(./oaire:funderName, ./oaire:funderIdentifier, ./oaire:funderIdentifier/substring-after(., 'doi.org/')) = ('Wellcome Trust', '10.13039/100010269')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varWT, ./oaire:awardNumber)"/>
                </oaf:projectid>
            </xsl:when>

         </xsl:choose>
         </xsl:for-each>

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

<!-- oaf:journal -->
<!-- ISSN  is erroneously stuffed in alternateIdentifier, should be put into relatedIdentifier -->
<!-- check/ensure that oaf:journal is not prepared for a journal itself, perhaps limit to Rothamsted -->
<!-- Huelva marks L, E ISSNs as ISSNs, with mark within field in spaces or after blanc -->
<!-- Qeios declares many records as text, although many seem to be definitions which are also related to 'journal' volumes/issues -->
         <xsl:if test="($varCobjCategory = '0001' or contains(//dri:recordIdentifier, 'www.qeios.com')) and (//*[local-name() = 'alternateIdentifier']/@alternateIdentifierType[. = 'ISSN'] or //*[local-name() = 'relatedIdentifier'][lower-case(@relationType) = 'ispartof'][@relatedIdentifierType = 'ISSN'])">
                  <oaf:journal>
                         <xsl:attribute name="issn" select="(//*[local-name() = 'alternateIdentifier'][./@alternateIdentifierType = 'ISSN'], //*[local-name() = 'relatedIdentifier'][lower-case(./@relationType) = 'ispartof'][./@relatedIdentifierType = 'ISSN'])[1]/concat(substring(., 1, 4), '-', substring(., string-length(.)-3, 4))"/>
                         <xsl:if test="//*[local-name() = 'citationVolume']">
                                   <xsl:attribute name="vol" select="//*[local-name() = 'citationVolume']"/>
                         </xsl:if>
                         <xsl:if test="//*[local-name() = 'citationIssue']">
                                   <xsl:attribute name="iss" select="//*[local-name() = 'citationIssue']"/>
                         </xsl:if>
                         <xsl:if test="//*[local-name() = 'citationStartPage']">
                                   <xsl:attribute name="sp" select="//*[local-name() = 'citationStartPage']"/>
                         </xsl:if>
                         <xsl:if test="//*[local-name() = 'citationEndPage']">
                                   <xsl:attribute name="ep" select="//*[local-name() = 'citationEndPage']"/>
                         </xsl:if>
                         <xsl:value-of select="((//*[local-name() = 'citationTitle'], //dc:source[//oaf:datasourceprefix[.='od______1514']]))"/>
                  </oaf:journal>
         </xsl:if>

         <xsl:if test="$varCobjCategory = '0001' and //*[local-name() = 'relatedIdentifier']/@relatedIdentifierType[. = ('ISSN', 'PISSN', 'EISSN', 'LISSN')]">
                  <oaf:journal>
         <xsl:for-each select="//*[local-name() = 'relatedIdentifier'][@relatedIdentifierType[. = ('ISSN', 'PISSN', 'EISSN', 'LISSN')]][@relationType/lower-case(.) = 'ispartof']">

                         <xsl:attribute name="{./@relatedIdentifierType}" select="./concat(substring(., 1, 4), '-', substring(., string-length(.)-3, 4))"/>
<!-- -->
         </xsl:for-each>

                         <xsl:if test="//*[local-name() = 'citationVolume']">
                                   <xsl:attribute name="vol" select="//*[local-name() = 'citationVolume']"/>
                         </xsl:if>
                         <xsl:if test="//*[local-name() = 'citationIssue']">
                                   <xsl:attribute name="iss" select="//*[local-name() = 'citationIssue']"/>
                         </xsl:if>
                         <xsl:if test="//*[local-name() = 'citationStartPage']">
                                   <xsl:attribute name="sp" select="//*[local-name() = 'citationStartPage']"/>
                         </xsl:if>
                         <xsl:if test="//*[local-name() = 'citationEndPage']">
                                   <xsl:attribute name="ep" select="//*[local-name() = 'citationEndPage']"/>
                         </xsl:if>
                         <xsl:value-of select="((//oaire:citationTitle))"/>
                  </oaf:journal>
         </xsl:if>

<!-- oaf:fulltext-->
<xsl:if test="//*[local-name() = 'file']">
       <oaf:fulltext>
                 <xsl:value-of select="//*[local-name() = 'file']"/>
       </oaf:fulltext>
</xsl:if>

      </metadata>
      <xsl:copy-of select="//*[local-name() = 'about']" />
   </record>
  </xsl:template>

<xsl:template match="node()|@*">
     <xsl:copy  copy-namespaces="no">
       <xsl:apply-templates select="node()|@*"/>
     </xsl:copy>
 </xsl:template>

  <xsl:template match="//*[local-name() = 'metadata']//*[local-name() = 'resource']">
    <xsl:copy>
       <xsl:apply-templates select="node()|@*"/>
     </xsl:copy>
  </xsl:template>

<!--
  <xsl:template match="//*[local-name() = 'resource']/*[local-name()='alternateIdentifiers']">
    <xsl:copy copy-namespaces="no">
     <xsl:copy-of select="./*[not($varCobjCategory = '0001' and ./@alternateIdentifierType = 'ISSN')]"  copy-namespaces="no"/>

    <xsl:if test="//*[local-name() = 'resource']/*[local-name()='identifier'][lower-case(@identifierType)='handle']">
          <datacite:alternateIdentifier>
          <xsl:attribute name="alternateIdentifierType">
             <xsl:value-of select="'URL'"/>
          </xsl:attribute>
              <xsl:value-of
                  select="//oaire:resource/(datacite:identifier[./@identifierType = 'handle' and not(contains(., '://hdl.handle.net/'))]/concat('http://hdl.handle.net/', .), datacite:identifier[contains(., '://hdl.handle.net/')])" />
          </datacite:alternateIdentifier>
    </xsl:if>

    <xsl:if test="//*[local-name() = 'resource']/*[local-name()='identifier'][@identifierType='URN']">
      <xsl:element name="alternateIdentifier" namespace="http://www.openarchives.org/OAI/2.0/">
          <xsl:attribute name="alternateIdentifierType">
             <xsl:value-of select="'URL'"/>
          </xsl:attribute>
              <xsl:value-of
                  select="concat('http://nbn-resolving.org/', //*[local-name() = 'resource']/*[local-name()='identifier'])" />
      </xsl:element>
    </xsl:if>

    <xsl:if test="//*[local-name() = 'resource']/*[local-name()='identifier'][@identifierType='DOI']">
          <datacite:alternateIdentifier>
          <xsl:attribute name="alternateIdentifierType">
             <xsl:value-of select="'URL'"/>
          </xsl:attribute>
              <xsl:value-of
                  select="//oaire:resource/(datacite:identifier[./@identifierType = 'doi' and not(contains(., '://dx.doi.org/'))]/concat('http://dx.doi.org/', .), datacite:identifier[contains(., '://dx.doi.org/')])" />
          </datacite:alternateIdentifier>
    </xsl:if>
    </xsl:copy>
  </xsl:template>
-->

  <xsl:template match="//*[local-name() = 'resource']/*[local-name()='identifier']">
<!-- cut off DOI resolver prefix to just get the number part -->
<!--
  <xsl:if test=".[@identifierType='DOI'][contains(., '://dx.doi.org/')]">
    <xsl:copy copy-namespaces="no">
        <xsl:attribute name="identifierType">
           <xsl:value-of select="'DOI'"/>
        </xsl:attribute>
            <xsl:value-of
                select="substring-after(., '://dx.doi.org/')" />
    </xsl:copy>
 </xsl:if>
  <xsl:if test=".[@identifierType='HANDLE'][contains(., '://hdl.handle.net/')]">
    <xsl:copy copy-namespaces="no">
        <xsl:attribute name="identifierType">
           <xsl:value-of select="'handle'"/>
        </xsl:attribute>
            <xsl:value-of
                select="substring-after(., '://hdl.handle.net/')" />
    </xsl:copy>
 </xsl:if>
     <xsl:copy-of select=".[not(contains(., '://dx.doi.org/') or contains(., 'hdl.handle.net/'))]"  copy-namespaces="no"/>


    <xsl:if test="not(//*[local-name() = 'resource']/*[local-name()='alternateIdentifiers'])">
      <xsl:element name="alternateIdentifiers" namespace="http://www.openarchives.org/OAI/2.0/">
    <xsl:if test=".[@identifierType='Handle']">
    <xsl:element name="alternateIdentifier" namespace="http://www.openarchives.org/OAI/2.0/">
        <xsl:attribute name="alternateIdentifierType">
           <xsl:value-of select="'URL'"/>
        </xsl:attribute>
            <xsl:value-of
                select="concat('http://hdl.handle.net/', .)" />
    </xsl:element>
    </xsl:if>
    <xsl:if test=".[@identifierType='URN']">
    <xsl:element name="alternateIdentifier" namespace="http://www.openarchives.org/OAI/2.0/">
        <xsl:attribute name="alternateIdentifierType">
           <xsl:value-of select="'URL'"/>
        </xsl:attribute>
            <xsl:value-of
                select="concat('http://nbn-resolving.org/', .)" />
    </xsl:element>
    </xsl:if>
    <xsl:if test=".[@identifierType='DOI']">
    <xsl:element name="alternateIdentifier" namespace="http://www.openarchives.org/OAI/2.0/">
        <xsl:attribute name="alternateIdentifierType">
           <xsl:value-of select="'URL'"/>
        </xsl:attribute>
            <xsl:value-of
                select="concat('http://dx.doi.org/', .)" />
    </xsl:element>
    </xsl:if>

    </xsl:element>
    </xsl:if>
-->



<!-- funding -->
<xsl:for-each select="//*[local-name()='fundingReference']">
<!-- FP7 -->
     <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100004963', '10.13039/100011199') and matches(./*[local-name()='awardNumber'], '.*(^|[^\d])\d\d\d\d\d\d($|[^\d]).*')">
          <oaf:projectid>
               <xsl:value-of select="concat($varFP7, replace(./*[local-name()='awardNumber'], '.*(^|[^\d])(\d\d\d\d\d\d)($|[^\d]).*', '$2'))"/>
          </oaf:projectid>
     </xsl:if>
<!-- H2020 -->
     <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/100010661') and matches(./*[local-name()='awardNumber'], '.*(^|[^\d])\d\d\d\d\d\d($|[^\d]).*')">
          <oaf:projectid>
               <xsl:value-of select="concat($varH2020, replace(./*[local-name()='awardNumber'], '.*(^|[^\d])(\d\d\d\d\d\d)($|[^\d]).*', '$2'))"/>
          </oaf:projectid>
     </xsl:if>
<!-- AKA -->
<xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100002341') or contains(./*[local-name()='funderName'], 'Suomen Akatemia') or contains(./*[local-name()='funderName'], 'Academy of Finland')">
     <oaf:projectid>
          <xsl:value-of select="concat($varAKA, ./*[local-name()='awardNumber'])"/>
     </oaf:projectid>
</xsl:if>
<!-- ARC -->
<xsl:if test="(substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100000923') or contains(./*[local-name()='funderName'], 'Australian Research Council')) and matches(./*[local-name()='awardNumber'], '^\d{6}$')">
     <oaf:projectid>
          <xsl:value-of select="concat($varAKA, replace(./*[local-name()='awardNumber'], '.*(^\d{6}$).*', '$2'))"/>
     </oaf:projectid>
</xsl:if>
<!-- CONICYT -->
<xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100002848') or contains(./*[local-name()='funderName'], 'Comisión Nacional de Investigación Científica y Tecnológica') or contains(./*[local-name()='funderName'], 'CONICYT')">
     <oaf:projectid>
          <xsl:value-of select="concat($varCONICYT, ./*[local-name()='awardNumber'])"/>
     </oaf:projectid>
</xsl:if>
<!-- DFG -->
<xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100001659') or contains(./*[local-name()='funderName'], 'Deutsche Forschungsgemeinschaft') or contains(./*[local-name()='funderName'], 'DFG')">
     <oaf:projectid>
          <xsl:value-of select="concat($varDFG, ./*[local-name()='awardNumber'])"/>
     </oaf:projectid>
</xsl:if>
<!-- FCT -->
<xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100001871') or contains(./*[local-name()='funderName'], 'Fundação para a Ciência e a Tecnologia')">
     <oaf:projectid>
          <xsl:value-of select="concat($varFCT, ./*[local-name()='awardNumber'])"/>
     </oaf:projectid>
</xsl:if>
<!-- FWF -->
<xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100002428') or contains(./*[local-name()='funderName'], 'Fonds zur Förderung der Wissenschaftlichen Forschung') or contains(./*[local-name()='funderName'], 'Austrian Science Fund')">
     <oaf:projectid>
          <xsl:value-of select="concat($varFCT, ./*[local-name()='awardNumber'])"/>
     </oaf:projectid>
</xsl:if>
<!-- MESTD -->
<xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100001871') or (contains(./*[local-name()='funderName'], 'Ministarstvo Prosvete, Nauke i Tehnolo') and contains(./*[local-name()='funderName'], 'kog Razvoja')) or contains(./*[local-name()='funderName'], 'MESTD')">
     <oaf:projectid>
          <xsl:value-of select="concat($varMESTD, ./*[local-name()='awardNumber'])"/>
     </oaf:projectid>
</xsl:if>
<!-- MZOS -->
<xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100006588') or contains(./*[local-name()='funderName'], 'Ministarstvo Znanosti, Obrazovanja i Sporta') or contains(./*[local-name()='funderName'], 'Ministry of Science, Education and Sports')">
     <oaf:projectid>
          <xsl:value-of select="concat($varMZOS, ./*[local-name()='awardNumber'])"/>
     </oaf:projectid>
</xsl:if>
<!-- NHMRC -->
<xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100000925') or contains(./*[local-name()='funderName'], 'National Health and Medical Research Council') or contains(./*[local-name()='funderName'], 'NHMRC')">
     <oaf:projectid>
          <xsl:value-of select="concat($varNHMRC, ./*[local-name()='awardNumber'])"/>
     </oaf:projectid>
</xsl:if>
<!-- NIH -->
<xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/100000002') or contains(./*[local-name()='funderName'], 'National Institutes of Health')">
     <oaf:projectid>
          <xsl:value-of select="concat($varNIH, ./*[local-name()='awardNumber'])"/>
     </oaf:projectid>
</xsl:if>
<!-- NSF -->
<xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org') = ('10.13039/100000001') or contains(./*[local-name()='funderName'], 'National Science Foundation')">
     <oaf:projectid>
          <xsl:value-of select="concat($varNSF, ./*[local-name()='awardNumber'])"/>
     </oaf:projectid>
</xsl:if>
<!-- NWO -->
<xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100003246') or contains(./*[local-name()='funderName'], 'Netherlands Organisation for Scientific Research') or contains(./*[local-name()='funderName'], 'Nederlandse Organisatie voor Wetenschappelijk Onderzoek')">
     <oaf:projectid>
          <xsl:value-of select="concat($varNWO, ./*[local-name()='awardNumber'])"/>
     </oaf:projectid>
</xsl:if>
<!-- RCUK -->
<xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100000690') or contains(./*[local-name()='funderName'], 'Research Councils UK') or contains(./*[local-name()='funderName'], 'RCUK')">
     <oaf:projectid>
          <xsl:value-of select="concat($varRCUK, ./*[local-name()='awardNumber'])"/>
     </oaf:projectid>
</xsl:if>
<!-- SFI -->
<xsl:if test="(substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100001602') or contains(./*[local-name()='funderName'], 'Science Foundation Ireland')) and matches(./*[local-name()='awardNumber'], '.*([\dA-Za-z\.\-]+/)+[\dA-Za-z\.\-]+.*')">
          <oaf:projectid>
               <xsl:value-of select="concat($varSFI, replace(./*[local-name()='awardNumber'], '.*(^|\s)(([\dA-Za-z\.\-]+/)+[\dA-Za-z\.\-]+)($|\s).*', '$2'))"/>
          </oaf:projectid>
     </xsl:if>
<!-- SNSF -->
<xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100001711') or contains(./*[local-name()='funderName'], 'Swiss National Science Foundation') or contains(./*[local-name()='funderName'], 'Schweizerischer Nationalfonds zur Förderung der Wissenschaftlichen Forschung') or (./*[local-name()='funderName' and . = 'SNSF'])">
     <oaf:projectid>
          <xsl:value-of select="concat($varSNSF, ./*[local-name()='awardNumber'])"/>
     </oaf:projectid>
</xsl:if>
<!-- TUBITAK -->
<xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100004410') or contains(./*[local-name()='funderName'], 'Turkish National Science and Research Council') or (contains(./*[local-name()='funderName'], 'Türkiye Bilimsel ve Teknolojik Ara') and contains(./*[local-name()='funderName'], 'rma Kurumu'))">
     <oaf:projectid>
          <xsl:value-of select="concat($varTUBITAK, ./*[local-name()='awardNumber'])"/>
     </oaf:projectid>
</xsl:if>
<!-- WT -->
<xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/100004440') or contains(./*[local-name()='funderName'], 'Wellcome Trust')">
     <oaf:projectid>
          <xsl:value-of select="concat($varWT, ./*[local-name()='awardNumber'])"/>
     </oaf:projectid>
</xsl:if>
</xsl:for-each>
 
  </xsl:template>

<!--
  <xsl:template match="//*[local-name()='language']">
         <oaf:language>
           <xsl:value-of select="TransformationFunction:convert($tf, //*[local-name()='language'], 'Languages')" />
         </oaf:language>
  </xsl:template>
-->

                       <xsl:template match="//*[local-name() = 'header']">
<xsl:copy>
     <xsl:apply-templates  select="node()|@*"/>
                             <xsl:element name="dr:dateOfTransformation">
                                 <xsl:value-of select="$transDate"/>
                             </xsl:element>
  </xsl:copy>
                        </xsl:template>

<!-- ToDo: drop URLs for DOIs? -->

</xsl:stylesheet>

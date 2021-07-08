<!-- from PROD 2021-06-14 -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.1" 
                             xmlns:dri="http://www.driver-repository.eu/namespace/dri" 
                             xmlns:dc="http://purl.org/dc/elements/1.1/" 
                             xmlns:oaf="http://namespace.openaire.eu/oaf" 
                             xmlns:dr="http://www.driver-repository.eu/namespace/dr" 
                             xmlns:xs="http://www.w3.org/2001/XMLSchema"
                             xmlns:xlink="http://www.w3.org/1999/xlink"
                             xmlns:transformExt="http://namespace.openaire.eu/java/org.apache.commons.codec.digest.DigestUtils" 
                             xmlns:TransformationFunction="eu.dnetlib.data.collective.transformation.core.xsl.ext.TransformationFunctionProxy"
                             extension-element-prefixes="transformExt TransformationFunction"
                             exclude-result-prefixes="transformExt TransformationFunction" >
                        <xsl:output indent="yes" omit-xml-declaration="yes"/>

<!-- 
  <xsl:param name="varHostedById" select="'opendoar____::908'"/>
  <xsl:param name="varHostedByName" select="'Europe PubMed Central'"/>
-->

  <xsl:param name="varOfficialName" />
  <xsl:param name="varDsType" />
  <xsl:param name="varDataSourceId" />
  <xsl:param name="varFP7FundRefDOI" select="'10.13039/501100004963'"/>
  <xsl:param name="varFP7OtherDOI" select="'10.13039/100011102'"/>
  <xsl:param name="varH2020FundRefDOI" select="'10.13039/501100007601'"/>
  <xsl:param name="varFP7" select="'corda_______::'"/>
  <xsl:param name="varH2020" select="'corda__h2020::'"/>
  <xsl:param name="epmcUrlPrefix" select="'http://europepmc.org/articles/'" />
  <xsl:param name="repoCode" select="substring-before(//*[local-name() = 'header']/*[local-name()='recordIdentifier'], ':')"/>

  <xsl:param name="index" select="0"/>
  <xsl:param name="transDate" select="current-dateTime()"/>
  <xsl:variable name="tf" select="TransformationFunction:getInstance()"/>
<xsl:variable name="year" select="format-number( ( //*[local-name()='article-meta']//*[local-name()='pub-date'][@pub-type='epub']/*[local-name()='year'] | //*[local-name()='article-meta']//*[local-name()='pub-date'][@date-type='pub' and @publication-format='electronic']/*[local-name()='year']), '0000')" />
  <xsl:variable name="month" select="format-number( (//*[local-name()='article-meta']//*[local-name()='pub-date'][@pub-type='epub']/*[local-name()='month'] | //*[local-name()='article-meta']//*[local-name()='pub-date'][@date-type='pub' and @publication-format='electronic']/*[local-name()='month']), '00')" />
  <xsl:variable name="day" select="format-number( (//*[local-name()='article-meta']//*[local-name()='pub-date'][@pub-type='epub']/*[local-name()='day'] | //*[local-name()='article-meta']//*[local-name()='pub-date'][@date-type='pub' and @publication-format='electronic']/*[local-name()='day']), '00')" />

               <xsl:template name="terminate">
                	<xsl:message terminate="yes">
                             	record is not compliant, transformation is interrupted.
                	</xsl:message>
               </xsl:template>

                        <xsl:template match="/">
                              <record>
                                  <xsl:apply-templates select="//*[local-name() = 'header']" />
                                  <metadata>
                                    <xsl:if test="not(//*[local-name() = 'article-meta']//*[local-name()='article-title'][string-length(normalize-space(.))> 0])">
                                                         <xsl:call-template name="terminate"/>
                                    </xsl:if>
<!-- in journal.fi xml:lang of translated titles is not within the trans-title element but within the surrounding trans-title-group element (which just contains 1 trans-title element) -->
<!--
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name() = 'article-meta']//*[local-name()=('article-title', 'trans-title-group')][string-length(normalize-space(.))> 0]"/>
                                            <xsl:with-param name="targetElement" select="'dc:title'"/>
                                    </xsl:call-template>
-->
                                    <xsl:call-template name="title">
                                            <xsl:with-param name="sourceElement" select="//*[local-name() = 'article-meta']/*[local-name()='title-group']//*[local-name()=('article-title', 'trans-title', 'subtitle', 'trans-subtitle')]"/>
                                    </xsl:call-template>

                                    <xsl:call-template name="authors">
<!--                                             
                                            <xsl:with-param name="sourceElement" select="//*[local-name() = 'article-meta']/*[local-name() = 'contrib-group']/*[local-name() = 'contrib'][@contrib-type='author'][not(exists(child::*:collab))]"/>
-->
                                            <xsl:with-param name="sourceElement" select="//*[local-name() = 'article-meta']/*[local-name() = 'contrib-group'][@content-type='author']/*[local-name() = 'contrib']"/>
                                    </xsl:call-template>
<!--                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:contributor"/>
                                            <xsl:with-param name="targetElement" select="'dc:contributor'"/>
                                    </xsl:call-template>
-->
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='article-meta']/*[local-name()=('abstract', 'trans-abstract')]"/>
                                            <xsl:with-param name="targetElement" select="'dc:description'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='article-categories']//*[local-name()='subject']"/>
                                            <xsl:with-param name="targetElement" select="'dc:subject'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='kwd-group']//*[local-name()='kwd']"/>
                                            <xsl:with-param name="targetElement" select="'dc:subject'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='publisher']/*[local-name()='publisher-name']"/>
                                            <xsl:with-param name="targetElement" select="'dc:publisher'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='journal-meta']//*[local-name()='journal-title']"/>
                                            <xsl:with-param name="targetElement" select="'dc:source'"/>
                                    </xsl:call-template>
                                    <xsl:element name="dc:language">
                                             <xsl:value-of select="//*[local-name()='metadata']//*[local-name()='article']/@xml:lang" />
                                    </xsl:element>
                                    <xsl:element name="dc:identifier">
                                            <xsl:value-of select="//*[local-name()='article-meta']/*[local-name()='self-uri'][contains(./@xlink:href, '/view/')]/@xlink:href" />
                                    </xsl:element>
                                    <xsl:element name="oaf:dateAccepted">
<!--
                                               <xsl:value-of select="//*[local-name()='article-meta']//*[local-name()='pub-date'][@date-type='pub' and @publication-format='epub' and string-length(normalize-space(./*[local-name()='year'])) = 4]/concat(./*[local-name()='year'], '-', ./*[local-name()='month'], '-', ./*[local-name()='day'])" />

                                                <xsl:value-of select="TransformationFunction:Convert($tf, //*[local-name()='article-meta']//*[local-name()='pub-date'][@date-type='pub' and @publication-format='epub'], 'DateISO8601', 'yyyy-MM-dd', 'min()')" />

                                                <xsl:value-of select="//*[local-name()='article-meta']//*[local-name()='pub-date'][@date-type='pub' and @publication-format='epub' and string-length(normalize-space(./*[local-name()='year'])) = 4]/replace(concat(./*[local-name()='year'], '-', ./*[local-name()='month'], '-', ./*[local-name()='day']), '-(\d)([-$])', '-0$1$2')" />
                                               <xsl:value-of select="//*[local-name()='article-meta']//*[local-name()='pub-date'][@date-type='pub' and @publication-format='epub' and string-length(normalize-space(./*[local-name()='year'])) = 4]/
                                                                concat(./*[local-name()='year'], '-',  
                                                                            substring(concat('0', ./*[local-name()='month'], '1'), string-length(./*[local-name()='month']), 2), '-', 
                                                                            substring(concat('0', ./*[local-name()='day'], '1'), string-length(./*[local-name()='day']), 2))" />
-->
<xsl:value-of select="//*[local-name()='article-meta']//*[local-name()='pub-date'][@date-type='pub' and @publication-format='epub' and string-length(normalize-space(./*[local-name()='year'])) = 4]/
                                                                concat(./*[local-name()='year'], '-',  
                                                                            substring(concat('0', ./*[local-name()='month'], '1'), string-length(./*[local-name()='month']) idiv 2 + 1, 2), '-',  
                                                                            substring(concat('0', ./*[local-name()='day'], '1'), string-length(./*[local-name()='day']) idiv 2 +1, 2))" />

                                    </xsl:element>





             <xsl:for-each select="//*[local-name()='article-meta']//*[local-name()='pub-date'][@date-type='pub' and @publication-format='epub']">
                   <xsl:choose>
                           <xsl:when test="./*[local-name()='year' and string-length(normalize-space(.)) = 4] and ./*[local-name()='month'  and string-length(normalize-space(.)) = 2] and ./*[local-name()='day'  and string-length(normalize-space(.)) = 2]">
                                 <dc:date>
                                          <xsl:value-of select="concat(./*[local-name()='year'], '-', ./*[local-name()='month'], '-', ./*[local-name()='day'])"/>
                                 </dc:date>
                           </xsl:when>
                           <xsl:when test="./*[local-name()='year'  and string-length(normalize-space(.)) = 4] and ./*[local-name()='month'  and string-length(normalize-space(.)) = 2]">
                                 <dc:date>
                                          <xsl:value-of select="concat(./*[local-name()='year'], '-', ./*[local-name()='month'])"/>
                                 </dc:date>
                           </xsl:when>
                           <xsl:when test="./*[local-name()='year'   and string-length(normalize-space(.)) = 4]">
                                 <dc:date>
                                          <xsl:value-of select="./*[local-name()='year']"/>
                                 </dc:date>
                           </xsl:when>
                   </xsl:choose>
            </xsl:for-each>

                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='custom-meta-group']/*[local-name()='custom-meta'][./@specific-use='access-right']/*[local-name()='meta-value'], //*[local-name()='permissions']/*[local-name()='copyright-statement']"/>
                                            <xsl:with-param name="targetElement" select="'dc:rights'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='permissions']/*[local-name()='license']/@xlink:href"/>
                                            <xsl:with-param name="targetElement" select="'oaf:license'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='fn-group']//*[local-name()='fn']"/>
                                            <xsl:with-param name="targetElement" select="'dc:relation'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="identifiers">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='article-id']"/>
                                    </xsl:call-template>

            <xsl:for-each select="//*[local-name()='article-meta']/*[local-name()='self-uri'][not(./@content-type = 'application/pdf')]/@xlink:href">
                        <oaf:identifier>
                                    <xsl:attribute name="identifierType">
                                               <xsl:text>landingPage</xsl:text>
                                    </xsl:attribute>
                                    <xsl:value-of select="."/>
                        </oaf:identifier>
            </xsl:for-each>
            <xsl:for-each select="//*[local-name()='article-meta']/*[local-name()='self-uri' and ./@content-type='application/pdf' and //oaf:datasourceprefix = ('ambientesust', 'qualityinedu')]/@xlink:href/replace(., '/view/', '/download/')">
                        <oaf:fulltext>
                                    <xsl:value-of select="."/>
                        </oaf:fulltext>
            </xsl:for-each>

             <xsl:for-each select="//*[local-name()='award-group'][.//*[local-name()='institution-id'][ends-with(., $varFP7FundRefDOI) or ends-with(., $varFP7OtherDOI)]]">
               <xsl:if test="./*[local-name()='award-id'][matches(normalize-space(.), '(^\d\d\d\d\d\d$)', 'i')]">
                   <oaf:projectid>
                    <xsl:value-of select="concat($varFP7, ./*[local-name()='award-id'])"/>
                </oaf:projectid>
              </xsl:if>
            </xsl:for-each>
             <xsl:for-each select="//*[local-name()='award-group'][.//*[local-name()='institution-id'][ends-with(., $varH2020FundRefDOI)]]">
               <xsl:if test="./*[local-name()='award-id'][matches(normalize-space(.), '(^\d\d\d\d\d\d$)', 'i')]">
                   <oaf:projectid>
                    <xsl:value-of select="concat($varH2020, ./*[local-name()='award-id'])"/>
                </oaf:projectid>
              </xsl:if>
            </xsl:for-each>

<!-- -->
<xsl:variable name='varRights' select="distinct-values((for $i in (
     //*[local-name()='custom-meta-group']/*[local-name()='custom-meta'][./@specific-use='access-right']/*[local-name()=('meta-value', 'meta-name')], 
     //*[local-name() = 'article-meta']/*[local-name() = 'permissions']/*[local-name() = 'license']/@xlink:href, 
     //*[local-name() = 'article-meta']/*[local-name() = 'permissions']/*[local-name() = 'free_to_read' 
          and not( ./@start_date[(xs:date( max( (string(.), '0001-01-01') ) ) gt current-date())]) 
          and not( ./@end_date[(xs:date( max( (string(.), '0001-01-01') ) ) lt current-date())])]/'open', 
     //*[local-name() = 'article-meta']/*[local-name() = 'permissions']/*[local-name() = 'free_to_read' 
          and (( ./@start_date[(xs:date( max( (string(.), '0001-01-01') ) ) gt current-date())]) 
          or ( ./@end_date[(xs:date( max( (string(.), '0001-01-01') ) ) lt current-date())]))]/'embargo')
     return TransformationFunction:convertString($tf, normalize-space($i), 'AccessRights')))" />

<!--
and not((xs:date( max( (start_date, '0001-01-01') ) ) gt current-date()))
//*[local-name() = 'article-meta']/*[local-name() = 'permissions']/*[local-name() = 'free_to_read' and and not((xs:date( max( (./@start_date, '0001-01-01') ) ) gt current-date()))]/'open'
-->

<oaf:accessrights>
     <xsl:choose>
          <xsl:when test="$varRights[. = 'EMBARGO']">
               <xsl:value-of select="'EMBARGO'"/>
          </xsl:when>
          <xsl:when test="$varRights[. != 'UNKNOWN']">
               <xsl:value-of select="$varRights[. != 'UNKNOWN'][1]"/>
          </xsl:when>
          <xsl:otherwise>
               <xsl:value-of select="$varRights[1]"/>
          </xsl:otherwise>
     </xsl:choose>
</oaf:accessrights>

<!--
<oaf:accessrights>
               <xsl:value-of select="$varRights[1]"/>
</oaf:accessrights>

<xsl:element name="oaf:accessrights">
     <xsl:value-of select="(//*[local-name()='custom-meta-group']/*[local-name()='custom-meta'][./@specific-use='access-right']/*[local-name()=('meta-value', 'meta-name')], 
                                             //*[local-name() = 'article-meta']/*[local-name() = 'permissions']/*[local-name() = 'license']/@xlink:href)/TransformationFunction:convertString($tf, ., 'AccessRights')" />
</xsl:element>
-->

<!--
                                  <xsl:element name="dr:CobjCategory">
                                             <xsl:variable name='varCobjCategory' select="TransformationFunction:convertString($tf, //*[local-name() = 'article-meta']/*[local-name() = 'custom-meta-group']/*[local-name() = 'custom-meta'][./@specific-use='resource-type']/*[local-name()='meta-value'], 'TextTypologies')" />
                                             <xsl:variable name='varSuperType' select="TransformationFunction:convertString($tf, $varCobjCategory, 'SuperTypes')" />
                                                           <xsl:attribute name="type" select="$varSuperType"/>
                                                           <xsl:value-of select="$varCobjCategory" />
                                  </xsl:element>

<xsl:variable name='varCobjCatLst' select="for $i in (
          //*[local-name() = 'article-meta']/*[local-name() = 'custom-meta-group']/*[local-name() = 'custom-meta'][./@specific-use='resource-type']/*[local-name()=('meta-value', 'meta-name')], 
          //*[local-name() = 'article']/@article-type)
          return TransformationFunction:convertString($tf, normalize-space($i), 'TextTypologies')" />
 -->

<xsl:variable name='varTypLst' select="distinct-values((//*[local-name() = 'article-meta']/*[local-name() = 'custom-meta-group']/*[local-name() = 'custom-meta'][./@specific-use='resource-type']/*[local-name()=('meta-value', 'meta-name')], 
          //*[local-name() = 'article']/@article-type))"/>
<xsl:variable name='varCobjCatLst' select="distinct-values((for $i in $varTypLst
          return TransformationFunction:convertString($tf, normalize-space($i), 'TextTypologies')))" />
<xsl:variable name='varCobjSupLst' select="for $i in $varCobjCatLst
          return concat($i, '###', TransformationFunction:convertString($tf, normalize-space($i), 'SuperTypes'))" />
<dr:CobjCategory>
     <xsl:choose>
          <xsl:when test="count($varCobjSupLst[not(substring-after(., '###') = 'other') and not(substring-before(., '###') = ('0038', '0039', '0040'))]) > 0">
               <xsl:variable name='varCobjSup' select="$varCobjSupLst[not(substring-after(., '###') = 'other') and not(substring-before(., '###') = ('0038', '0039', '0040'))][1]" />
               <xsl:attribute name="type" select="substring-after($varCobjSup, '###')"/>
               <xsl:value-of select="substring-before($varCobjSup, '###')" />
          </xsl:when>
          <xsl:when test="count($varCobjSupLst[not(substring-after(., '###') = 'other')]) > 0">
               <xsl:variable name='varCobjSup' select="$varCobjSupLst[not(substring-after(., '###') = 'other')][1]" />
               <xsl:attribute name="type" select="substring-after($varCobjSup, '###')"/>
               <xsl:value-of select="substring-before($varCobjSup, '###')" />
          </xsl:when>
          <xsl:when test="count($varCobjSupLst[not(substring-before(., '###') = ('0020', '0000'))]) > 0">
               <xsl:variable name='varCobjSup' select="$varCobjSupLst[not(substring-before(., '###') = ('0020', '0000'))][1]" />
               <xsl:attribute name="type" select="substring-after($varCobjSup, '###')"/>
               <xsl:value-of select="substring-before($varCobjSup, '###')" />
          </xsl:when>
          <xsl:when test="count($varCobjSupLst[not(substring-before(., '###') = ('0000'))]) > 0">
               <xsl:variable name='varCobjSup' select="$varCobjSupLst[not(substring-before(., '###') = ('0000'))][1]" />
               <xsl:attribute name="type" select="substring-after($varCobjSup, '###')"/>
               <xsl:value-of select="substring-before($varCobjSup, '###')" />
          </xsl:when>
          <xsl:otherwise>
               <xsl:attribute name="type" select="'other'"/>
               <xsl:value-of select="'0000'" />
          </xsl:otherwise>
     </xsl:choose>
</dr:CobjCategory>

<!--
             <xsl:for-each select="$varCobjSupLst">
                    <dc:type>
                    <xsl:value-of select="."/>
                </dc:type>
             </xsl:for-each>
-->

             <xsl:for-each select="$varTypLst">
                    <dc:type>
                    <xsl:value-of select="."/>
                </dc:type>
             </xsl:for-each>

<!--
             <xsl:for-each select="(//*[local-name()='article']/@article-type, //*[local-name() = 'custom-meta' and ./@specific-use = 'resource-type']/*[local-name() = ('meta-value', 'meta-name')])">
                    <dc:type>
                    <xsl:value-of select="."/>
                </dc:type>
             </xsl:for-each>
-->

<oaf:language>
              <xsl:value-of select="TransformationFunction:convertString($tf, //*[local-name()='metadata']//*[local-name()='article']/@xml:lang, 'Languages')" />
</oaf:language>

<!-- review status -->
<!-- ToDo: 
review status
~ ask Journal.fi to put it elsewhere 
~ evaluate article-version (no example found yet)
subject/kwd:
~ handle thesauri (no example found yet)
relations:
~ handle fn (no example found yet)
-->
<!--
<xsl:variable name="varRefereedConvt" select="for $i in (
              //*[local-name() = 'article-meta']/*[local-name() = 'custom-meta-group']/*[local-name() = 'custom-meta'][./@specific-use='resource-type']/*[local-name()=('meta-value', 'meta-name')], 
              //*[local-name() = 'article']/@article-type) 
               return TransformationFunction:convertString($tf, normalize-space($i), 'ReviewLevels')"/>
-->

<xsl:variable name="varRefereedConvt" select="for $i in ($varTypLst) 
               return TransformationFunction:convertString($tf, normalize-space($i), 'ReviewLevels')"/>
<xsl:variable name="varRefereedDescp" select="//*[local-name() = 'article-meta']/*[local-name() = ('abstract', 'trans-abstract')][matches(lower-case(.), '^\s*(.p.\s*)?refereed\s*article.*')]/'0001'"/>
<xsl:variable name="varRefereedSubjt" select="//*[local-name() = 'article-categories' and contains(//dri:recordIdentifier, 'oai:journal.fi')]/*[local-name() = 'subj-group' and ./@subj-group-type='heading']/*[local-name() = 'subject' and . = 'Peer reviewed articles']/'0001'"/>
<xsl:variable name="varRefereed" select="($varRefereedConvt, $varRefereedDescp, $varRefereedSubjt)"/>
<!--
          <oaf:refereed>
               <xsl:value-of select="$varRefereedDescp"/>
          </oaf:refereed>
          <oaf:refereed>
               <xsl:value-of select="$varRefereed"/>
          </oaf:refereed>
          <oaf:refereed>
               <xsl:value-of select="count($varRefereed[. = '0001']) > 0"/>
          </oaf:refereed>
 -->
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

                                    <xsl:call-template name="journal">
                                            <xsl:with-param name="journalTitle" select="//*[local-name()='journal-meta']//*[local-name()='journal-title']"/>
                                            <xsl:with-param name="issn" select="//*[local-name()='journal-meta']/*[local-name()='issn'][@pub-type='ppub']"/>
                                            <xsl:with-param name="eissn" select="//*[local-name()='journal-meta']/*[local-name()='issn'][@pub-type='epub']"/>
                                            <xsl:with-param name="vol" select="//*[local-name()='article-meta']/*[local-name()='volume']"/>
                                            <xsl:with-param name="issue" select="//*[local-name()='article-meta']/*[local-name()='issue']"/>
                                            <xsl:with-param name="sp" select="//*[local-name()='article-meta']/*[local-name()='fpage']"/>
                                            <xsl:with-param name="ep" select="//*[local-name()='article-meta']/*[local-name()='lpage']"/>
                                    </xsl:call-template>
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

         <xsl:template name="allElements">
             <xsl:param name="sourceElement"/>
             <xsl:param name="targetElement"/>
             <xsl:for-each select="$sourceElement">
                <xsl:element name="{$targetElement}">
             <xsl:if test="(.[@xml:lang] or ..[@xml:lang]) and $targetElement = ('dc:title', 'dc:description', 'dc:subject')">
                    <xsl:attribute name="xml:lang">
                    <xsl:value-of select="(./@xml:lang, ../@xml:lang)[1]"/>
                    </xsl:attribute>
                </xsl:if>
                    <xsl:value-of select="normalize-space(.)"/>
                </xsl:element>
             </xsl:for-each>
          </xsl:template>

         <xsl:template name="title">
             <xsl:param name="sourceElement"/>
             <xsl:for-each select="$sourceElement">
                <xsl:element name="dc:title">
             <xsl:if test=".[@xml:lang] or ..[@xml:lang]">
                    <xsl:attribute name="xml:lang">
                    <xsl:value-of select="(./@xml:lang, ../@xml:lang)[1]"/>
                    </xsl:attribute>
                </xsl:if>
                    <xsl:value-of select="string-join((., ./following-sibling::*[local-name() = ('subtitle', 'trans-subtitle')])/normalize-space(.), ': ')"/>
                </xsl:element>
             </xsl:for-each>
          </xsl:template>

          <xsl:template name="journal">
             <xsl:param name="journalTitle"/>
             <xsl:param name="issn"/>
             <xsl:param name="eissn"/>
             <xsl:param name="vol"/>
             <xsl:param name="issue"/>
             <xsl:param name="sp"/>
             <xsl:param name="ep"/>
                <xsl:element name="oaf:journal">
                    <xsl:attribute name="issn">
                    <xsl:value-of select="normalize-space($issn)"/>
                    </xsl:attribute>
                    <xsl:attribute name="eissn">
                    <xsl:value-of select="normalize-space($eissn)"/>
                    </xsl:attribute>
                    <xsl:attribute name="vol">
                    <xsl:value-of select="normalize-space($vol)"/>
                    </xsl:attribute>
                    <xsl:attribute name="iss">
                    <xsl:value-of select="normalize-space($issue)"/>
                    </xsl:attribute>
                    <xsl:attribute name="sp">
                    <xsl:value-of select="normalize-space($sp)"/>
                    </xsl:attribute>
                    <xsl:attribute name="ep">
                    <xsl:value-of select="normalize-space($ep)"/>
                    </xsl:attribute>
                    <xsl:value-of select="normalize-space($journalTitle)"/>
                </xsl:element>
          </xsl:template>


          <xsl:template name="identifiers">
             <xsl:param name="sourceElement"/>
             <xsl:if test="string-length($sourceElement[@pub-id-type='doi']) gt 0">
                <xsl:element name="oaf:identifier">
                         <xsl:attribute name="identifierType">
                               <xsl:text>doi</xsl:text>
                         </xsl:attribute>
                         <xsl:value-of select="$sourceElement[@pub-id-type='doi']"/>
                </xsl:element>
                </xsl:if>
          </xsl:template>


          <xsl:template name="authors">
             <xsl:param name="sourceElement"/>
             <xsl:for-each select="$sourceElement">
                <xsl:element name="dc:creator">
                    <xsl:if test="./*[local-name()='contrib-id'][@contrib-id-type='orcid']">
                         <xsl:attribute name="nameIdentifierScheme">
                             <xsl:text>ORCID</xsl:text>
                         </xsl:attribute>
                         <xsl:attribute name="schemeURI">
                             <xsl:text>http://orcid.org/</xsl:text>
                         </xsl:attribute> 
                         <xsl:attribute name="nameIdentifier">
                             <xsl:value-of select="substring-after(./*[local-name()='contrib-id'][@contrib-id-type='orcid'], 'http://orcid.org/')"/>
                         </xsl:attribute> 
                    </xsl:if>
                    <xsl:value-of select="concat(normalize-space(./*[local-name()='name']/*[local-name()='surname']), ', ', normalize-space(./*[local-name()='name']/*[local-name()='given-names']))"/>
                </xsl:element>
             </xsl:for-each>              
          </xsl:template>



                       <xsl:template match="//*[local-name() = 'header']">
<xsl:copy>
     <xsl:apply-templates  select="node()|@*"/>
                             <xsl:element name="dr:dateOfTransformation">
                                 <xsl:value-of select="$transDate"/>
                             </xsl:element>
  </xsl:copy>
                        </xsl:template>


                       <xsl:template match="node()|@*">
                            <xsl:copy>
                                 <xsl:apply-templates select="node()|@*"/>
                            </xsl:copy>
                       </xsl:template>
                    </xsl:stylesheet>
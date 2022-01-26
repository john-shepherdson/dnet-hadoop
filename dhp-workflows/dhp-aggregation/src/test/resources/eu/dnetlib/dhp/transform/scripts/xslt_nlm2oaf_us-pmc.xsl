<!-- for adaptation , 2021-06-14 PROD -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
     xmlns:oaire="http://namespace.openaire.eu/schema/oaire/"
     xmlns:vocabulary="http://eu/dnetlib/transform/clean"
     xmlns:dateCleaner="http://eu/dnetlib/transform/dateISO"
     xmlns:oaf="http://namespace.openaire.eu/oaf"
     xmlns:datacite="http://datacite.org/schema/kernel-4"
     xmlns:dri="http://www.driver-repository.eu/namespace/dri"
     xmlns:xs="http://www.w3.org/2001/XMLSchema"
     xmlns:dr="http://www.driver-repository.eu/namespace/dr"
     xmlns:dc="http://purl.org/dc/elements/1.1/"
     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
     exclude-result-prefixes="xsl vocabulary dateCleaner" 
     version="2.0">

     <xsl:param name="varOfficialName" />
     <xsl:param name="varDsType" />
     <xsl:param name="varDataSourceId" />
     <xsl:output indent="yes" omit-xml-declaration="yes" />
     <xsl:param name="varHostedById" select="&apos;opendoar____::908&apos;" />
     <xsl:param name="varHostedByName" select="&apos;Europe PubMed Central&apos;" />

     <xsl:param name="varFP7FundRefDOI" select="&apos;10.13039/501100004963&apos;" />
     <xsl:param name="varH2020FundRefDOI" select="&apos;10.13039/501100007601&apos;" />
     <xsl:param name="varFP7" select="&apos;corda_______::&apos;" />
     <xsl:param name="varH2020" select="&apos;corda__h2020::&apos;" />
     <xsl:param name="epmcUrlPrefix" select="&apos;http://europepmc.org/articles/&apos;" />
     <xsl:param name="repoCode" select="substring-before(//*[local-name() = &apos;header&apos;]/*[local-name()=&apos;recordIdentifier&apos;], &apos;:&apos;)" />
     <xsl:param name="index" select="0" />
     <xsl:param name="transDate" select="current-dateTime()" />
     <xsl:variable name="year" select="format-number( ( //*[local-name()=&apos;article-meta&apos;]//*[local-name()=&apos;pub-date&apos;][@pub-type=&apos;epub&apos;]/*[local-name()=&apos;year&apos;] | //*[local-name()=&apos;article-meta&apos;]//*[local-name()=&apos;pub-date&apos;][@date-type=&apos;pub&apos; and @publication-format=&apos;electronic&apos;]/*[local-name()=&apos;year&apos;]), &apos;0000&apos;)" />
     <xsl:variable name="month" select="format-number( (//*[local-name()=&apos;article-meta&apos;]//*[local-name()=&apos;pub-date&apos;][@pub-type=&apos;epub&apos;]/*[local-name()=&apos;month&apos;] | //*[local-name()=&apos;article-meta&apos;]//*[local-name()=&apos;pub-date&apos;][@date-type=&apos;pub&apos; and @publication-format=&apos;electronic&apos;]/*[local-name()=&apos;month&apos;]), &apos;00&apos;)" />
     <xsl:variable name="day" select="format-number( (//*[local-name()=&apos;article-meta&apos;]//*[local-name()=&apos;pub-date&apos;][@pub-type=&apos;epub&apos;]/*[local-name()=&apos;day&apos;] | //*[local-name()=&apos;article-meta&apos;]//*[local-name()=&apos;pub-date&apos;][@date-type=&apos;pub&apos; and @publication-format=&apos;electronic&apos;]/*[local-name()=&apos;day&apos;]), &apos;00&apos;)" />

     <xsl:template name="terminate">
          <xsl:message terminate="yes">
                             	record is not compliant, transformation is interrupted.
          </xsl:message>
     </xsl:template>

     <xsl:template match="/">
          <record>
               <xsl:apply-templates select="//*[local-name() = &apos;header&apos;]" />
               <metadata>
                    <xsl:if test="not(//*[local-name() = &apos;article-meta&apos;]//*[local-name()=&apos;article-title&apos;][string-length(normalize-space(.))&gt; 0])">
                         <xsl:call-template name="terminate" />
                    </xsl:if>
                    <xsl:call-template name="allElements">
                         <xsl:with-param name="sourceElement" select="//*[local-name() = &apos;article-meta&apos;]//*[local-name()=&apos;article-title&apos;][string-length(normalize-space(.))&gt; 0]" />
                         <xsl:with-param name="targetElement" select="&apos;dc:title&apos;" />
                    </xsl:call-template>
                    <xsl:call-template name="authors">
                         <!--                                             
                                            <xsl:with-param name="sourceElement" select="//*[local-name() = 'contrib'][@contrib-type='author']"/>
-->
                         <xsl:with-param name="sourceElement" select="//*[local-name() = &apos;article-meta&apos;]/*[local-name() = &apos;contrib-group&apos;]/*[local-name() = &apos;contrib&apos;][@contrib-type=&apos;author&apos;][not(exists(child::*:collab))][./*[local-name()=&apos;name&apos;] or ./*[local-name()=&apos;name-alternatives&apos;]/*[local-name()=&apos;name&apos;]][string-length(.//*[local-name()=&apos;surname&apos;]) + string-length(.//*[local-name()=&apos;given-names&apos;]) &gt; 0]" />
                    </xsl:call-template>                    <!--                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:contributor"/>
                                            <xsl:with-param name="targetElement" select="'dc:contributor'"/>
                                    </xsl:call-template>
-->
                    <xsl:call-template name="allElements">
                         <xsl:with-param name="sourceElement" select="//*[local-name()=&apos;article-meta&apos;]/*[local-name()=&apos;abstract&apos;]" />
                         <xsl:with-param name="targetElement" select="&apos;dc:description&apos;" />
                    </xsl:call-template>
                    <xsl:call-template name="allElements">
                         <xsl:with-param name="sourceElement" select="//*[local-name()=&apos;article-categories&apos;]//*[local-name()=&apos;subject&apos;]" />
                         <xsl:with-param name="targetElement" select="&apos;dc:subject&apos;" />
                    </xsl:call-template>
                    <xsl:call-template name="allElements">
                         <xsl:with-param name="sourceElement" select="//*[local-name()=&apos;kwd-group&apos;  and not(lower-case(@kwd-group-type)=(&apos;mesh&apos;, &apos;ocis&apos;))]//*[local-name()=&apos;kwd&apos;]" />
                         <xsl:with-param name="targetElement" select="&apos;dc:subject&apos;" />
                    </xsl:call-template>
                    <xsl:for-each select="//*[local-name()=&apos;kwd-group&apos; and lower-case(@kwd-group-type)=&apos;mesh&apos; and ./*[local-name()=&apos;kwd&apos;]]">
                         <xsl:for-each select="./*[local-name()=&apos;kwd&apos;]">
                              <dc:subject>
                                   <xsl:attribute name="subjectScheme" select="&apos;mesh&apos;" />
                                   <xsl:attribute name="schemeURI" select="&apos;http://www.nlm.nih.gov/mesh/&apos;" />
                                   <xsl:attribute name="valueURI" select="&apos;&apos;" />
                                   <xsl:value-of select="./concat(&apos;mesh:&apos;, replace(., &apos;mesh (.*)$&apos;, &apos;$1&apos;))" />
                              </dc:subject>
                         </xsl:for-each>
                    </xsl:for-each>
                    <xsl:for-each select="//*[local-name()=&apos;kwd-group&apos; and lower-case(@kwd-group-type)=&apos;ocis&apos; and ./*[local-name()=&apos;kwd&apos;]]">
                         <xsl:for-each select="./*[local-name()=&apos;kwd&apos;]">
                              <dc:subject>
                                   <xsl:attribute name="subjectScheme" select="&apos;ocis&apos;" />
                                   <xsl:attribute name="schemeURI" select="&apos;&apos;" />
                                   <xsl:attribute name="valueURI" select="&apos;&apos;" />
                                   <xsl:value-of select="./concat(&apos;ocis:&apos;, .)" />
                              </dc:subject>
                         </xsl:for-each>
                    </xsl:for-each>
                    <xsl:call-template name="allElements">
                         <xsl:with-param name="sourceElement" select="//*[local-name()=&apos;publisher&apos;]/*[local-name()=&apos;publisher-name&apos;]" />
                         <xsl:with-param name="targetElement" select="&apos;dc:publisher&apos;" />
                    </xsl:call-template>
                    <xsl:call-template name="allElements">
                         <xsl:with-param name="sourceElement" select="//*[local-name()=&apos;journal-meta&apos;]//*[local-name()=&apos;journal-title&apos;]" />
                         <xsl:with-param name="targetElement" select="&apos;dc:source&apos;" />
                    </xsl:call-template>
                    <xsl:call-template name="allElements">
                         <xsl:with-param name="sourceElement" select="//*[local-name() = &apos;article-meta&apos;]/(*[local-name() = &apos;article-version-alternatives&apos;]/*[local-name() = &apos;article-version&apos;], *[local-name() = &apos;article-version&apos;])/concat(&apos;article-version (&apos;, @article-version-type, &apos;) &apos;, .)" />
                         <xsl:with-param name="targetElement" select="&apos;dc:source&apos;" />
                    </xsl:call-template>
                    <xsl:element name="dc:language">
                         <xsl:text>eng</xsl:text>
                    </xsl:element>
                    <xsl:element name="dc:identifier">
                         <xsl:value-of select="concat($epmcUrlPrefix, //*[local-name()=&apos;article-id&apos;][@pub-id-type=&apos;pmcid&apos;])" />
                    </xsl:element>
                    <xsl:element name="oaf:fulltext">
                         <xsl:value-of select="concat($epmcUrlPrefix, //*[local-name()=&apos;article-id&apos;][@pub-id-type=&apos;pmcid&apos;])" />
                    </xsl:element>
                    <xsl:element name="oaf:dateAccepted">
                         <xsl:choose>
                              <xsl:when test="//*[local-name()=&apos;article-meta&apos;]//*[local-name()=&apos;pub-date&apos;][@pub-type=&apos;epub&apos;] or //*[local-name()=&apos;article-meta&apos;]//*[local-name()=&apos;pub-date&apos;][@date-type=&apos;pub&apos; and @publication-format=&apos;electronic&apos;]">
                                   <xsl:if test="string(number($month)) eq &apos;NaN&apos;">
                                        <xsl:value-of select="concat($year, &apos;-&apos;, &apos;01&apos;, &apos;-&apos;, &apos;01&apos;)" />
                                   </xsl:if>
                                   <xsl:if test="string(number($month)) != &apos;NaN&apos;">
                                        <xsl:value-of select="concat($year, &apos;-&apos;, $month, &apos;-&apos;, &apos;01&apos;)" />
                                   </xsl:if>
                              </xsl:when>
                              <xsl:otherwise>
                                   <xsl:value-of select="concat(//*[local-name()=&apos;article-meta&apos;]//*[local-name()=&apos;pub-date&apos;][@pub-type=&apos;ppub&apos;]/*[local-name()=&apos;year&apos;], &apos;-01-01&apos;)" />
                              </xsl:otherwise>
                         </xsl:choose>
                    </xsl:element>
                    <xsl:call-template name="allElements">
                         <xsl:with-param name="sourceElement" select="distinct-values(//*[local-name()=&apos;permissions&apos;]/*[local-name()=&apos;copyright-statement&apos;])" />
                         <xsl:with-param name="targetElement" select="&apos;dc:rights&apos;" />
                    </xsl:call-template>
                    <xsl:call-template name="allElements">
                         <xsl:with-param name="sourceElement" select="distinct-values(//*[local-name()=&apos;permissions&apos;]/*[local-name()=&apos;license&apos;])" />
                         <xsl:with-param name="targetElement" select="&apos;dc:rights&apos;" />
                    </xsl:call-template>
                    <xsl:call-template name="allElements">
                         <xsl:with-param name="sourceElement" select="//*[local-name()=&apos;fn-group&apos;]//*[local-name()=&apos;fn&apos;]" />
                         <xsl:with-param name="targetElement" select="&apos;dc:relation&apos;" />
                    </xsl:call-template>
                    <xsl:call-template name="identifiers">
                         <xsl:with-param name="sourceElement" select="//*[local-name()=&apos;article-id&apos;]" />
                    </xsl:call-template>
                    <xsl:for-each select="//*[local-name()=&apos;award-group&apos;][.//*[local-name()=&apos;institution-id&apos;][ends-with(., $varFP7FundRefDOI)]]">
                         <xsl:if test="./*[local-name()=&apos;award-id&apos;][matches(normalize-space(.), &apos;(^\d\d\d\d\d\d$)&apos;, &apos;i&apos;)]">
                              <oaf:projectid>
                                   <xsl:value-of select="concat($varFP7, ./*[local-name()=&apos;award-id&apos;])" />
                              </oaf:projectid>
                         </xsl:if>
                    </xsl:for-each>
                    <xsl:for-each select="//*[local-name()=&apos;award-group&apos;][.//*[local-name()=&apos;institution-id&apos;][ends-with(., $varH2020FundRefDOI)]]">
                         <xsl:if test="./*[local-name()=&apos;award-id&apos;][matches(normalize-space(.), &apos;(^\d\d\d\d\d\d$)&apos;, &apos;i&apos;)]">
                              <oaf:projectid>
                                   <xsl:value-of select="concat($varH2020, ./*[local-name()=&apos;award-id&apos;])" />
                              </oaf:projectid>
                         </xsl:if>
                    </xsl:for-each>
                    <xsl:element name="oaf:accessrights">
                         <xsl:text>OPEN</xsl:text>
                    </xsl:element>
                    <xsl:element name="dr:CobjCategory">
                         <xsl:attribute name="type" select="&apos;publication&apos;" />
                         <xsl:text>0001</xsl:text>
                    </xsl:element>
                    <dc:type>
                         <xsl:value-of select="//*[local-name() = &apos;article&apos;]/@article-type" />
                    </dc:type>                    


                    <xsl:variable name="varRefereedConvt" select="for $i in (//*[local-name() = 'resource']/*[local-name() = ('resourceType', 'version')]/(., @uri)) 
                      return vocabulary:clean( normalize-space($i), 'dnet:review_levels')"/>

<!--                    <xsl:variable name="varRefereedConvt" select="for $i in distinct-values((//*[local-name() = &apos;article&apos;]/@article-type, //oai:setSpec)) 
               return TransformationFunction:convertString($tf, normalize-space($i), &apos;ReviewLevels&apos;)" />
-->
                    <xsl:choose>
                         <xsl:when test="count($varRefereedConvt[. = &apos;0001&apos;]) &gt; 0">
                              <oaf:refereed>
                                   <xsl:value-of select="&apos;0001&apos;" />
                              </oaf:refereed>
                         </xsl:when>
                         <xsl:when test="//*[local-name() = &apos;article-meta&apos;]/*[local-name() = &apos;article-id&apos;][@pub-id-type=&apos;doi&apos;][matches(., &apos;^(https?://(dx\.)?doi.org/)?10\.12688/(f1000research|wellcomeopenres|aasopenres|gatesopenres|hrbopenres)\.\d*(\.\d*|-\d*\.v\d*)$&apos;)]">
                              <oaf:refereed>
                                   <xsl:value-of select="&apos;0001&apos;" />
                              </oaf:refereed>
                         </xsl:when>
                         <xsl:when test="//*[local-name() = &apos;article-meta&apos;]/*[local-name() = (&apos;abstract&apos;, &apos;trans-abstract&apos;)][matches(lower-case(.), &apos;^\s*(.p.\s*)?refereed\s*article.*&apos;)]">
                              <oaf:refereed>
                                   <xsl:value-of select="&apos;0001&apos;" />
                              </oaf:refereed>
                         </xsl:when>
                         <xsl:when test="//*[local-name() = &apos;article&apos;]/*[local-name() = (&apos;back&apos;, &apos;front&apos;)]/*[local-name() = (&apos;fn-group&apos;, &apos;notes&apos;)][
               matches(lower-case(.), &apos;.*peer[\.\-_/\s\(\)]?review\s*under\s*responsibility\s*of.*&apos;) or 
               matches(lower-case(.), &apos;.*peer[\.\-_/\s\(\)]*review\s*information.*&apos;) or
               matches(lower-case(.), &apos;.*the\s*peer[\.\-_/\s\(\)]*review\s*history\s*for\s*this\s*article\s*is\s*available\s*at .*&apos;) or
               matches(lower-case(.), &apos;.*provenance\s*and\s*peer[\.\-_/\s\(\)]*review.*&apos;) or
               matches(lower-case(.), &apos;.*externally\s*peer[\.\-_/\s\(\)]*reviewed.*&apos;) or
               matches(lower-case(.), &apos;.*peer[\.\-_/\s\(\)]*reviewed\s*by.*&apos;) or
               matches(lower-case(.), &apos;.*refereed\s*anonymously.*&apos;) or
               matches(lower-case(.), &apos;.*peer\s*reviewer\s*reports\s*are\s*available.*&apos;) or
               matches(lower-case(.), &apos;.*\[.*peer[\s\-\._]*review\s*:.*\].*&apos;) or
               matches(lower-case(.), &apos;.*\[.*referees\s*:.*\].*&apos;) or
               matches(lower-case(.), &apos;^\s*plagiarism[\s\-\._]check.*&apos;) or
               matches(lower-case(.), &apos;^\s*peer[\s\-\._]*review.*&apos;) or
               matches(lower-case(.), &apos;^\s*(open\s*peer[\s\-\._]*|p-)reviewer.*&apos;) or
               matches(lower-case(.), &apos;^\s*(open\s*peer[\s\-\._]*|p-)review\s*reports?.*&apos;)]">
                              <oaf:refereed>
                                   <xsl:value-of select="&apos;0001&apos;" />
                              </oaf:refereed>
                         </xsl:when>
                         <xsl:when test="//*[local-name() = (&apos;article-meta&apos;, &apos;app&apos;, &apos;app-group&apos;)]/*[local-name() = &apos;supplementary-material&apos;]/*[local-name() = &apos;media&apos;][
               matches(lower-case(.), &apos;.*peer\s*review\s*file.*&apos;)]">
                              <oaf:refereed>
                                   <xsl:value-of select="&apos;0001&apos;" />
                              </oaf:refereed>
                         </xsl:when>
                         <xsl:when test="//*[local-name() = &apos;article-meta&apos;]/*[local-name() = &apos;contrib-group&apos;]
               [./@role/lower-case(.) = (&apos;reviewer&apos;, &apos;solicited external reviewer&apos;) or 
               ./*[local-name() = &apos;contrib&apos;][./@role/lower-case(.) = (&apos;reviewer&apos;, &apos;solicited external reviewer&apos;) or ./*[local-name() = &apos;role&apos; and lower-case(.) = (&apos;reviewer&apos;, &apos;solicited external reviewer&apos;)] or ./@contrib-type/lower-case(.) = &apos;reviewer&apos;]]">
                              <oaf:refereed>
                                   <xsl:value-of select="&apos;0001&apos;" />
                              </oaf:refereed>
                         </xsl:when>
                         <xsl:when test="count($varRefereedConvt[. = &apos;0002&apos;]) &gt; 0">
                              <oaf:refereed>
                                   <xsl:value-of select="&apos;0002&apos;" />
                              </oaf:refereed>
                         </xsl:when>
                         <xsl:when test="//*[local-name() = (&apos;related-article&apos;)][./@related-article-type = (&apos;peer-reviewed-article&apos;, &apos;reviewed-article&apos;)]">
                              <oaf:refereed>
                                   <xsl:value-of select="&apos;0002&apos;" />
                              </oaf:refereed>
                         </xsl:when>
                         <xsl:when test="//*[local-name() = &apos;article-meta&apos;][./*[local-name() = &apos;article-version-alternatives&apos;]/*[local-name() = &apos;article-version&apos; and . = &apos;preprint&apos;] or ./*[local-name() = &apos;article-version&apos; and . = &apos;preprint&apos;]]">
                              <oaf:refereed>
                                   <xsl:value-of select="&apos;0002&apos;" />
                              </oaf:refereed>
                         </xsl:when>
                    </xsl:choose>
                    <xsl:call-template name="journal">
                         <xsl:with-param name="journalTitle" select="//*[local-name()=&apos;journal-meta&apos;]//*[local-name()=&apos;journal-title&apos;]" />
                         <xsl:with-param name="issn" select="//*[local-name()=&apos;journal-meta&apos;]/*[local-name()=&apos;issn&apos;][@pub-type=&apos;ppub&apos;]" />
                         <xsl:with-param name="eissn" select="//*[local-name()=&apos;journal-meta&apos;]/*[local-name()=&apos;issn&apos;][@pub-type=&apos;epub&apos;]" />
                         <xsl:with-param name="vol" select="//*[local-name()=&apos;article-meta&apos;]/*[local-name()=&apos;volume&apos;]" />
                         <xsl:with-param name="issue" select="//*[local-name()=&apos;article-meta&apos;]/*[local-name()=&apos;issue&apos;]" />
                         <xsl:with-param name="sp" select="//*[local-name()=&apos;article-meta&apos;]/*[local-name()=&apos;fpage&apos;]" />
                         <xsl:with-param name="ep" select="//*[local-name()=&apos;article-meta&apos;]/*[local-name()=&apos;lpage&apos;]" />
                    </xsl:call-template>
                    <oaf:hostedBy>
                         <xsl:attribute name="name">
                              <xsl:value-of select="$varHostedByName" />
                         </xsl:attribute>
                         <xsl:attribute name="id">
                              <xsl:value-of select="$varHostedById" />
                         </xsl:attribute>
                    </oaf:hostedBy>
                    <oaf:collectedFrom>
                         <xsl:attribute name="name">
                              <xsl:value-of select="$varOfficialName" />
                         </xsl:attribute>
                         <xsl:attribute name="id">
                              <xsl:value-of select="$varDataSourceId" />
                         </xsl:attribute>
                    </oaf:collectedFrom>
                    <xsl:for-each select="//*[local-name() = &apos;article&apos;]/*[local-name() = (&apos;back&apos;, &apos;front&apos;)]/*[local-name() = &apos;fn-group&apos;]/*[local-name() = &apos;fn&apos;][matches(lower-case(.), &apos;country(/territory)? of origin:?\s*[A-Za-z\-]+&apos;)]">
                         <oaf:country>
<!--
               <xsl:value-of select="TransformationFunction:convertString($tf, replace(lower-case(.), '^(.|\s)*country(/territory)? of origin:?\s+([A-Za-z\-,\(\)]+(\s+[A-Za-z\-,\(\)]+)*)(.|\s)*$', '$3'), 'Countries')"/>
-->
<!-- ACz, 2021-06-14
                              <xsl:value-of select="TransformationFunction:convertString($tf, normalize-space(substring(substring-after(lower-case(.), &apos;of origin&apos;), 2)), &apos;Countries&apos;)" />
-->
                         <xsl:value-of select="vocabulary:clean( normalize-space(substring(substring-after(lower-case(.), &apos;of origin&apos;), 2)), 'dnet:countries')"/>
                    </oaf:country>
                    </xsl:for-each>
               </metadata>
               <xsl:copy-of select="//*[local-name() = &apos;about&apos;]" />
          </record>
     </xsl:template>
     <xsl:template name="allElements">
          <xsl:param name="sourceElement" />
          <xsl:param name="targetElement" />
          <xsl:for-each select="$sourceElement">
               <xsl:element name="{$targetElement}">
                    <xsl:value-of select="normalize-space(.)" />
               </xsl:element>
          </xsl:for-each>
     </xsl:template>
     <xsl:template name="journal">
          <xsl:param name="journalTitle" />
          <xsl:param name="issn" />
          <xsl:param name="eissn" />
          <xsl:param name="vol" />
          <xsl:param name="issue" />
          <xsl:param name="sp" />
          <xsl:param name="ep" />
          <xsl:element name="oaf:journal">
               <xsl:attribute name="issn">
                    <xsl:value-of select="normalize-space($issn)" />
               </xsl:attribute>
               <xsl:attribute name="eissn">
                    <xsl:value-of select="normalize-space($eissn)" />
               </xsl:attribute>
               <xsl:attribute name="vol">
                    <xsl:value-of select="normalize-space($vol)" />
               </xsl:attribute>
               <xsl:attribute name="iss">
                    <xsl:value-of select="normalize-space($issue)" />
               </xsl:attribute>
               <xsl:attribute name="sp">
                    <xsl:value-of select="normalize-space($sp)" />
               </xsl:attribute>
               <xsl:attribute name="ep">
                    <xsl:value-of select="normalize-space($ep)" />
               </xsl:attribute>
               <xsl:value-of select="normalize-space($journalTitle)" />
          </xsl:element>
     </xsl:template>
     <xsl:template name="identifiers">
          <xsl:param name="sourceElement" />
          <xsl:element name="oaf:identifier">
               <xsl:attribute name="identifierType">
                    <xsl:text>doi</xsl:text>
               </xsl:attribute>
               <xsl:value-of select="$sourceElement[@pub-id-type=&apos;doi&apos;]" />
          </xsl:element>
          <xsl:element name="oaf:identifier">
               <xsl:attribute name="identifierType">
                    <xsl:text>pmc</xsl:text>
               </xsl:attribute>
               <xsl:value-of select="$sourceElement[@pub-id-type=&apos;pmcid&apos;]" />
          </xsl:element>
          <xsl:element name="oaf:identifier">
               <xsl:attribute name="identifierType">
                    <xsl:text>pmid</xsl:text>
               </xsl:attribute>
               <xsl:value-of select="$sourceElement[@pub-id-type=&apos;pmid&apos;]" />
          </xsl:element>
     </xsl:template>
     <xsl:template name="authors">
          <xsl:param name="sourceElement" />
          <xsl:for-each select="$sourceElement">
               <xsl:element name="dc:creator">
                    <xsl:if test="./*[local-name()=&apos;contrib-id&apos;][@contrib-id-type=&apos;orcid&apos;]">
                         <xsl:attribute name="nameIdentifierScheme">
                              <xsl:text>ORCID</xsl:text>
                         </xsl:attribute>
                         <xsl:attribute name="schemeURI">
                              <xsl:text>http://orcid.org/</xsl:text>
                         </xsl:attribute>
                         <xsl:attribute name="nameIdentifier">
                              <xsl:value-of select="substring-after(./*[local-name()=&apos;contrib-id&apos;][@contrib-id-type=&apos;orcid&apos;], &apos;http://orcid.org/&apos;)" />
                         </xsl:attribute>
                    </xsl:if>                    <!--
                    <xsl:value-of select="concat(normalize-space(./*[local-name()='name']/*[local-name()='surname']), ', ', normalize-space(./*[local-name()='name']/*[local-name()='given-names']))"/>
-->
                    <xsl:value-of select="concat(normalize-space(./(*[local-name()=&apos;name&apos;], *[local-name()=&apos;name-alternatives&apos;]/*[local-name()=&apos;name&apos;])/*[local-name()=&apos;surname&apos;]), &apos;, &apos;, normalize-space(./(*[local-name()=&apos;name&apos;], *[local-name()=&apos;name-alternatives&apos;]/*[local-name()=&apos;name&apos;])/*[local-name()=&apos;given-names&apos;]))" />
               </xsl:element>
          </xsl:for-each>
     </xsl:template>
     <xsl:template match="//*[local-name() = &apos;header&apos;]">
          <xsl:copy>
               <xsl:apply-templates select="node()|@*" />
               <xsl:element name="dr:dateOfTransformation">
                    <xsl:value-of select="$transDate" />
               </xsl:element>
          </xsl:copy>
     </xsl:template>
     <xsl:template match="node()|@*">
          <xsl:copy>
               <xsl:apply-templates select="node()|@*" />
          </xsl:copy>
     </xsl:template>
</xsl:stylesheet>
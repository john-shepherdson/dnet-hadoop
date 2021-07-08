<!-- from production 2021-0614 -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.1" 
                             xmlns:dri="http://www.driver-repository.eu/namespace/dri" 
                             xmlns:dc="http://purl.org/dc/elements/1.1/" 
                             xmlns:oaf="http://namespace.openaire.eu/oaf" 
                             xmlns:dr="http://www.driver-repository.eu/namespace/dr" 
                             xmlns:oai="http://www.openarchives.org/OAI/2.0/"
                             xmlns:transformExt="http://namespace.openaire.eu/java/org.apache.commons.codec.digest.DigestUtils" 
                             xmlns:TransformationFunction="eu.dnetlib.data.collective.transformation.core.xsl.ext.TransformationFunctionProxy"
                             extension-element-prefixes="transformExt TransformationFunction"
                             exclude-result-prefixes="transformExt TransformationFunction" >
                        <xsl:output indent="yes" omit-xml-declaration="yes"/>

  <xsl:param name="varHostedById" select="'opendoar____::908'"/>
  <xsl:param name="varHostedByName" select="'Europe PubMed Central'"/>

  <xsl:param name="varOfficialName" />
  <xsl:param name="varDsType" />
  <xsl:param name="varDataSourceId" />
  <xsl:param name="varFP7FundRefDOI" select="'10.13039/501100004963'"/>
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
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name() = 'article-meta']//*[local-name()='article-title'][string-length(normalize-space(.))> 0]"/>
                                            <xsl:with-param name="targetElement" select="'dc:title'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="authors">
<!--                                             
                                            <xsl:with-param name="sourceElement" select="//*[local-name() = 'contrib'][@contrib-type='author']"/>
-->
                                            <xsl:with-param name="sourceElement" select="//*[local-name() = 'article-meta']/*[local-name() = 'contrib-group']/*[local-name() = 'contrib'][@contrib-type='author'][not(exists(child::*:collab))][./*[local-name()='name'] or ./*[local-name()='name-alternatives']/*[local-name()='name']][string-length(.//*[local-name()='surname']) + string-length(.//*[local-name()='given-names']) > 0]"/>
                                    </xsl:call-template>
<!--                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:contributor"/>
                                            <xsl:with-param name="targetElement" select="'dc:contributor'"/>
                                    </xsl:call-template>
-->
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='article-meta']/*[local-name()='abstract']"/>
                                            <xsl:with-param name="targetElement" select="'dc:description'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='article-categories']//*[local-name()='subject']"/>
                                            <xsl:with-param name="targetElement" select="'dc:subject'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='kwd-group'  and not(lower-case(@kwd-group-type)=('mesh', 'ocis'))]//*[local-name()='kwd']"/>
                                            <xsl:with-param name="targetElement" select="'dc:subject'"/>
                                    </xsl:call-template>

<xsl:for-each select="//*[local-name()='kwd-group' and lower-case(@kwd-group-type)='mesh' and ./*[local-name()='kwd']]">
<xsl:for-each select="./*[local-name()='kwd']">
      <dc:subject>
                   <xsl:attribute name="subjectScheme" select="'mesh'"/>
                   <xsl:attribute name="schemeURI" select="'http://www.nlm.nih.gov/mesh/'"/>
                   <xsl:attribute name="valueURI" select="''"/>
                   <xsl:value-of select="./concat('mesh:', replace(., 'mesh (.*)$', '$1'))"/>
      </dc:subject>
</xsl:for-each>
</xsl:for-each>
<xsl:for-each select="//*[local-name()='kwd-group' and lower-case(@kwd-group-type)='ocis' and ./*[local-name()='kwd']]">
<xsl:for-each select="./*[local-name()='kwd']">
      <dc:subject>
                   <xsl:attribute name="subjectScheme" select="'ocis'"/>
                   <xsl:attribute name="schemeURI" select="''"/>
                   <xsl:attribute name="valueURI" select="''"/>
                   <xsl:value-of select="./concat('ocis:', .)"/>
      </dc:subject>
</xsl:for-each>
</xsl:for-each>

                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='publisher']/*[local-name()='publisher-name']"/>
                                            <xsl:with-param name="targetElement" select="'dc:publisher'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='journal-meta']//*[local-name()='journal-title']"/>
                                            <xsl:with-param name="targetElement" select="'dc:source'"/>
                                    </xsl:call-template>

                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name() = 'article-meta']/(*[local-name() = 'article-version-alternatives']/*[local-name() = 'article-version'], *[local-name() = 'article-version'])/concat('article-version (', @article-version-type, ') ', .)"/>
                                            <xsl:with-param name="targetElement" select="'dc:source'"/>
                                    </xsl:call-template>

                                    <xsl:element name="dc:language">
                                                                          <xsl:text>eng</xsl:text>
                                    </xsl:element>
                                    <xsl:element name="dc:identifier">
                                            <xsl:value-of select="concat($epmcUrlPrefix, //*[local-name()='article-id'][@pub-id-type='pmcid'])" />
                                    </xsl:element>
                                    <xsl:element name="oaf:fulltext">
                                            <xsl:value-of select="concat($epmcUrlPrefix, //*[local-name()='article-id'][@pub-id-type='pmcid'])" />
                                    </xsl:element>
                                    <xsl:element name="oaf:dateAccepted">
                                            <xsl:choose>
                                            <xsl:when test="//*[local-name()='article-meta']//*[local-name()='pub-date'][@pub-type='epub'] or //*[local-name()='article-meta']//*[local-name()='pub-date'][@date-type='pub' and @publication-format='electronic']" >
                                               <xsl:if test="string(number($month)) eq 'NaN'" >
                                               <xsl:value-of select="concat($year, '-', '01', '-', '01')" />
                                               </xsl:if>
                                               <xsl:if test="string(number($month)) != 'NaN'" >
                                               <xsl:value-of select="concat($year, '-', $month, '-', '01')" />
                                               </xsl:if>
                                            </xsl:when>
                                            <xsl:otherwise>
                                               <xsl:value-of select="concat(//*[local-name()='article-meta']//*[local-name()='pub-date'][@pub-type='ppub']/*[local-name()='year'], '-01-01')" />
                                            </xsl:otherwise>
                                            </xsl:choose>
                                    </xsl:element>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="distinct-values(//*[local-name()='permissions']/*[local-name()='copyright-statement'])"/>
                                            <xsl:with-param name="targetElement" select="'dc:rights'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="distinct-values(//*[local-name()='permissions']/*[local-name()='license'])"/>
                                            <xsl:with-param name="targetElement" select="'dc:rights'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='fn-group']//*[local-name()='fn']"/>
                                            <xsl:with-param name="targetElement" select="'dc:relation'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="identifiers">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='article-id']"/>
                                    </xsl:call-template>

             <xsl:for-each select="//*[local-name()='award-group'][.//*[local-name()='institution-id'][ends-with(., $varFP7FundRefDOI)]]">
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

                                  <xsl:element name="oaf:accessrights">
                                             <xsl:text>OPEN</xsl:text>
                                  </xsl:element>

                                  <xsl:element name="dr:CobjCategory">
                                             <xsl:attribute name="type" select="'publication'"/>
                                             <xsl:text>0001</xsl:text>
                                  </xsl:element>

<dc:type>
          <xsl:value-of select="//*[local-name() = 'article']/@article-type"/>
</dc:type>

<!-- custom-meta perhaps not used for types, then drop
<xsl:variable name='varTypLst' select="distinct-values((//*[local-name() = 'article-meta']/*[local-name() = 'custom-meta-group']/*[local-name() = 'custom-meta'][./@specific-use='resource-type']/*[local-name()=('meta-value', 'meta-name')], 
          //*[local-name() = 'article']/@article-type))"/>
<xsl:variable name='varTypLst' select="//*[local-name() = 'article']/@article-type"/>
-->
<!-- perhaps ensure that file indeed exists, e.g. as pdf etc -->
<!--
// reduce load for the big PubMed records by exchanging variables with choose
<xsl:variable name="varRefereedConvt" select="for $i in distinct-values((//*[local-name() = 'article']/@article-type, //oai:setSpec)) 
               return TransformationFunction:convertString($tf, normalize-space($i), 'ReviewLevels')"/>
<xsl:variable name="varRefereedDescp" select="//*[local-name() = 'article-meta']/*[local-name() = ('abstract', 'trans-abstract')][matches(lower-case(.), '^\s*(.p.\s*)?refereed\s*article.*')]/'0001'"/>
<xsl:variable name="varRefereedFnote" select="//*[local-name() = 'article']/*[local-name() = ('back', 'front')]/*[local-name() = ('fn-group', 'notes')][
               matches(lower-case(.), '.*peer[\.\-_/\s\(\)]?review\s*under\s*responsibility\s*of.*') or 
               matches(lower-case(.), '.*peer[\.\-_/\s\(\)]*review\s*information.*') or
               matches(lower-case(.), '.*the\s*peer[\.\-_/\s\(\)]*review\s*history\s*for\s*this\s*article\s*is\s*available\s*at .*') or
               matches(lower-case(.), '.*provenance\s*and\s*peer[\.\-_/\s\(\)]*review.*') or
               matches(lower-case(.), '.*externally\s*peer[\.\-_/\s\(\)]*reviewed.*') or
               matches(lower-case(.), '.*peer[\.\-_/\s\(\)]*reviewed\s*by.*') or
               matches(lower-case(.), '.*refereed\s*anonymously.*') or
               matches(lower-case(.), '.*peer\s*reviewer\s*reports\s*are\s*available.*')
               ]/'0001'"/>
<xsl:variable name="varRefereedReviw" select="//*[local-name() = ('article-meta', 'app', 'app-group')]/*[local-name() = 'supplementary-material']/*[local-name() = 'media'][
               matches(lower-case(.), '.*peer\s*review\s*file.*')]/'0001'"/>
<xsl:variable name="varRefereedReltn" select="//*[local-name() = ('related-article')][./@related-article-type = ('peer-reviewed-article', 'reviewed-article')]/'0002'"/>
<xsl:variable name="varRefereedCtRol" select="//*[local-name() = 'article-meta']/*[local-name() = 'contrib-group']
               [./@role/lower-case(.) = ('reviewer', 'solicited external reviewer') or 
               ./*[local-name() = 'contrib'][./@role/lower-case(.) = ('reviewer', 'solicited external reviewer') or ./*[local-name() = 'role' and lower-case(.) = ('reviewer', 'solicited external reviewer')] or ./@contrib-type/lower-case(.) = 'reviewer']]/'0001'"/>
<xsl:variable name="varRefereedVersn" select="//*[local-name() = 'article-meta'][./*[local-name() = 'article-version-alternatives']/*[local-name() = 'article-version' and . = 'preprint'] or ./*[local-name() = 'article-version' and . = 'preprint']]/'0002'"/>
<xsl:variable name="varRefereed" select="($varRefereedConvt, $varRefereedDescp, $varRefereedFnote, $varRefereedReviw, $varRefereedReltn, $varRefereedCtRol, $varRefereedVersn)"/>
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
-->
<xsl:variable name="varRefereedConvt" select="for $i in distinct-values((//*[local-name() = 'article']/@article-type, //oai:setSpec)) 
               return TransformationFunction:convertString($tf, normalize-space($i), 'ReviewLevels')"/>
<xsl:choose>
     <xsl:when test="count($varRefereedConvt[. = '0001']) > 0">
          <oaf:refereed>
               <xsl:value-of select="'0001'"/>
          </oaf:refereed>
     </xsl:when>

     <xsl:when test="//*[local-name() = 'article-meta']/*[local-name() = 'article-id'][@pub-id-type='doi'][matches(., '^(https?://(dx\.)?doi.org/)?10\.12688/(f1000research|wellcomeopenres|aasopenres|gatesopenres|hrbopenres)\.\d*(\.\d*|-\d*\.v\d*)$')]">
          <oaf:refereed>
               <xsl:value-of select="'0001'"/>
          </oaf:refereed>
     </xsl:when>

     <xsl:when test="//*[local-name() = 'article-meta']/*[local-name() = ('abstract', 'trans-abstract')][matches(lower-case(.), '^\s*(.p.\s*)?refereed\s*article.*')]">
          <oaf:refereed>
               <xsl:value-of select="'0001'"/>
          </oaf:refereed>
     </xsl:when>
     <xsl:when test="//*[local-name() = 'article']/*[local-name() = ('back', 'front')]/*[local-name() = ('fn-group', 'notes')][
               matches(lower-case(.), '.*peer[\.\-_/\s\(\)]?review\s*under\s*responsibility\s*of.*') or 
               matches(lower-case(.), '.*peer[\.\-_/\s\(\)]*review\s*information.*') or
               matches(lower-case(.), '.*the\s*peer[\.\-_/\s\(\)]*review\s*history\s*for\s*this\s*article\s*is\s*available\s*at .*') or
               matches(lower-case(.), '.*provenance\s*and\s*peer[\.\-_/\s\(\)]*review.*') or
               matches(lower-case(.), '.*externally\s*peer[\.\-_/\s\(\)]*reviewed.*') or
               matches(lower-case(.), '.*peer[\.\-_/\s\(\)]*reviewed\s*by.*') or
               matches(lower-case(.), '.*refereed\s*anonymously.*') or
               matches(lower-case(.), '.*peer\s*reviewer\s*reports\s*are\s*available.*') or
               matches(lower-case(.), '.*\[.*peer[\s\-\._]*review\s*:.*\].*') or
               matches(lower-case(.), '.*\[.*referees\s*:.*\].*') or
               matches(lower-case(.), '^\s*plagiarism[\s\-\._]check.*') or
               matches(lower-case(.), '^\s*peer[\s\-\._]*review.*') or
               matches(lower-case(.), '^\s*(open\s*peer[\s\-\._]*|p-)reviewer.*') or
               matches(lower-case(.), '^\s*(open\s*peer[\s\-\._]*|p-)review\s*reports?.*')]">
          <oaf:refereed>
               <xsl:value-of select="'0001'"/>
          </oaf:refereed>
     </xsl:when>
     <xsl:when test="//*[local-name() = ('article-meta', 'app', 'app-group')]/*[local-name() = 'supplementary-material']/*[local-name() = 'media'][
               matches(lower-case(.), '.*peer\s*review\s*file.*')]">
               <oaf:refereed>
                    <xsl:value-of select="'0001'"/>
               </oaf:refereed>
     </xsl:when>
     <xsl:when test="//*[local-name() = 'article-meta']/*[local-name() = 'contrib-group']
               [./@role/lower-case(.) = ('reviewer', 'solicited external reviewer') or 
               ./*[local-name() = 'contrib'][./@role/lower-case(.) = ('reviewer', 'solicited external reviewer') or ./*[local-name() = 'role' and lower-case(.) = ('reviewer', 'solicited external reviewer')] or ./@contrib-type/lower-case(.) = 'reviewer']]">
               <oaf:refereed>
                    <xsl:value-of select="'0001'"/>
               </oaf:refereed>
     </xsl:when>
     <xsl:when test="count($varRefereedConvt[. = '0002']) > 0">
               <oaf:refereed>
                    <xsl:value-of select="'0002'"/>
               </oaf:refereed>
     </xsl:when>
     <xsl:when test="//*[local-name() = ('related-article')][./@related-article-type = ('peer-reviewed-article', 'reviewed-article')]">
               <oaf:refereed>
                    <xsl:value-of select="'0002'"/>
               </oaf:refereed>
     </xsl:when>
     <xsl:when test="//*[local-name() = 'article-meta'][./*[local-name() = 'article-version-alternatives']/*[local-name() = 'article-version' and . = 'preprint'] or ./*[local-name() = 'article-version' and . = 'preprint']]">
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
               <xsl:value-of select="$varHostedByName"/>
            </xsl:attribute>
            <xsl:attribute name="id">
               <xsl:value-of select="$varHostedById"/>
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

<xsl:for-each select="//*[local-name() = 'article']/*[local-name() = ('back', 'front')]/*[local-name() = 'fn-group']/*[local-name() = 'fn'][matches(lower-case(.), 'country(/territory)? of origin:?\s*[A-Za-z\-]+')]">
         <oaf:country>
<!--
               <xsl:value-of select="TransformationFunction:convertString($tf, replace(lower-case(.), '^(.|\s)*country(/territory)? of origin:?\s+([A-Za-z\-,\(\)]+(\s+[A-Za-z\-,\(\)]+)*)(.|\s)*$', '$3'), 'Countries')"/>
-->
              <xsl:value-of select="TransformationFunction:convertString($tf, normalize-space(substring(substring-after(lower-case(.), 'of origin'), 2)), 'Countries')"/>
         </oaf:country>
</xsl:for-each>


                                  </metadata>
                                <xsl:copy-of select="//*[local-name() = 'about']" />
                              </record>

                        </xsl:template>

         <xsl:template name="allElements">
             <xsl:param name="sourceElement"/>
             <xsl:param name="targetElement"/>
             <xsl:for-each select="$sourceElement">
                <xsl:element name="{$targetElement}">
                    <xsl:value-of select="normalize-space(.)"/>
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
                <xsl:element name="oaf:identifier">
                         <xsl:attribute name="identifierType">
                               <xsl:text>doi</xsl:text>
                         </xsl:attribute>
                         <xsl:value-of select="$sourceElement[@pub-id-type='doi']"/>
                </xsl:element>
                <xsl:element name="oaf:identifier">
                         <xsl:attribute name="identifierType">
                               <xsl:text>pmc</xsl:text>
                         </xsl:attribute>
                         <xsl:value-of select="$sourceElement[@pub-id-type='pmcid']"/>
                </xsl:element>
                <xsl:element name="oaf:identifier">
                         <xsl:attribute name="identifierType">
                               <xsl:text>pmid</xsl:text>
                         </xsl:attribute>
                         <xsl:value-of select="$sourceElement[@pub-id-type='pmid']"/>
                </xsl:element>
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
<!--
                    <xsl:value-of select="concat(normalize-space(./*[local-name()='name']/*[local-name()='surname']), ', ', normalize-space(./*[local-name()='name']/*[local-name()='given-names']))"/>
-->
                    <xsl:value-of select="concat(normalize-space(./(*[local-name()='name'], *[local-name()='name-alternatives']/*[local-name()='name'])/*[local-name()='surname']), ', ', normalize-space(./(*[local-name()='name'], *[local-name()='name-alternatives']/*[local-name()='name'])/*[local-name()='given-names']))"/>
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

<!-- original: xslt_dc2oaf_narcis from PROD 2021-11-18 -->
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

  <xsl:param name="varOfficialName" />
  <xsl:param name="varDsType" />
  <xsl:param name="varDataSourceId" />
  <xsl:param name="varFP7" select="'corda_______::'"/>
  <xsl:param name="varH2020" select="'corda__h2020::'"/>

  <xsl:param name="repoCode" select="substring-before(//*[local-name() = 'header']/*[local-name()='recordIdentifier'], ':')"/>

  <xsl:param name="index" select="0"/>
  <xsl:param name="transDate" select="current-dateTime()"/>
  <xsl:variable name="tf" select="TransformationFunction:getInstance()"/>



<xsl:variable name="vCodes">
  <codes>
   <code key="uva" value="od_______323"   id="opendoar____::323" name="Universiteit van Amsterdam Digital Academic Repository"/>
   <code key="uvapub" value="od_______323"   id="opendoar____::323" name="Universiteit van Amsterdam Digital Academic Repository"/>
   <code key="vumc" value="od_______323"   id="opendoar____::323" name="Universiteit van Amsterdam Digital Academic Repository"/>
   <code key="cwi" value="od______2358" id="opendoar____::2358" name="Repository CWI Amsterdam"/> <!-- CWI -->
   <code key="eur" value="od______1113"  id="opendoar____::1113" name="Erasmus University Institutional Repository"/>
   <code key="wur" value="od_______370"  id="opendoar____::370" name="Wageningen Yield"/>
   <code key="uu" value="od_______101"    id="opendoar____::101" name="Utrecht University Repository"/>
   <code key="ru" value="od______1236"    id="opendoar____::1236" name="Radboud Repository"/> <!-- Radboud -->
   <code key="run" value="od______1236"  id="opendoar____::1236" name="Radboud Repository"/> <!-- Radboud -->
   <code key="uvt" value="od_______550"   id="opendoar____::550" name="Tilburg University Repository"/> <!-- Tilburg -->
   <code key="aup" value="od________19"   id="opendoar____::19" name="Amsterdam University Press Publications"/> <!-- amsterdam univ. press -->
   <code key="rug" value="od_______189"  id="opendoar____::189" name="University of Groningen Digital Archive"/> <!-- groningen -->
   <code key="dans" value="r384e1237760"  id="re3data_____::r3d100010214" name="EASY"/> <!--easy -->
   <code key="differ" value="differ______"  id="openaire____::75ee19e2-ff9e-47f7-bed6-2e3ee23e2b49" name="Dutch Institute for Fundamental Energy Research"/>
   <code key="kit" value="od______1423"  id="opendoar____::1423" name="Search4Dev"/> <!-- search4dev -->
   <code key="ul" value="od_______202"  id="opendoar____::202" name="Leiden University Repository"/> <!-- leiden -->
   <code key="um" value="od________83"  id="opendoar____::83" name="UM Publications"/>
   <code key="knaw" value="od______1476"  id="opendoar____::1476" name="KNAW Repository"/>
   <code key="vu" value="od_______369"  id="opendoar____::369" name="DSpace at VU"/>
   <code key="ut" value="od_______354"  id="opendoar____::354" name="Universiteit Twente Repository"/>
   <code key="hbo" value="hbo_________"  id="openaire____::79c8217f-00ee-4902-9743-9e11b4970c60" name="HBO Kennisbank"/>
   <code key="kim" value="kim_________"  id="openaire____::b1b15b72-bf0b-4f91-9f95-dab2e43d3eaa" name="Publicaties KiM"/>
   <code key="nivel" value="nivel_____nl"  id="driver______::daf0542d-1ef5-4f9d-80f1-62849b92aefa" name="NIVEL publications"/>
   <code key="ntrl" value="od_______913"  id="opendoar____::913" name="Naturalis Publications"/>
   <code key="nyenrode" value="nyenrode____"  id="openaire____::e57352f3-516b-42cb-b666-2480233c6513" name="Publications of the University Nyenrode"/>
   <code key="ou" value="od_______233"  id="opendoar____::233" name="DSpace at Open Universiteit Nederland "/>
   <code key="ptu" value="ptu_________"  id="openaire____::openaire____::f834f1fe-8198-4929-ac0b-b1c1bf166f38" name="Protestantse Theologische Universiteit"/>
   <code key="rivm" value="od_______881"  id="opendoar____::881" name="Web-based Archive of RIVM Publications"/>
   <code key="scp" value="scp_________"  id="openaire____::088a0087-4bc6-4c38-a052-b446c3b225a7" name="Sociaal en Cultureel Planbureau"/>
   <code key="swov" value="swov________"  id="openaire____::06d89df2-b613-4989-9dc3-f60f2fc593f6" name="Stichting Wetenschappelijk Onderzoek Verkeersveiligheid (SWOV) Library Repository"/>
   <code key="tno" value="tno_________"  id="openaire____::58fd0ad2-c476-11e5-80b3-0021e9e777ac" name="TNO Repository - hosted by TU Delft Library"/>
   <code key="tue" value="od_______567"  id="opendoar____::567" name="Repository TU/e"/>
   <code key="tuk" value="tuk_________"  id="openaire____::df55d991-1ebb-459c-aed6-559bcbb1d277::" name="Theological University Kampen"/>
   <code key="uvh" value="uvh_______nl"  id="driver______::a422c38b-73de-44bf-a340-a4fd5f0817ea" name="Universiteit voor Humanistiek"/>
   <code key="tua" value="tua_________"   id="openaire____::cd073e1e-2fe9-4ea7-aea5-dc6855c347f7" name="Theological University Apeldoorn"/>
   <code key="tud" value="od_______571"   id="opendoar____::571" name="TU Delft Repository"/>
   <code key="wodc" value="wodc______nl"   id="driver______::03c60250-9d65-44fe-85d3-23503b3303af" name="WODC Repository Ministerie van Veiligheid en Justitie"/> 
   <code key="unesco" value="unesco___ihe"  id="2877c7c4-b57a-4f62-9c16-d7faa5b0b98b" name="UNESCO-IHE Institute for Water Education"/>
  </codes>
 </xsl:variable>
<!-- not considered
hbo added
tno added
differ added
nyenrode added
beeldengeluis todo, not yet found in metadata
philips todo, not yet found in metadata
scp added
swov added
tuk added
tua added
ptu added
ut_restricted ? merge with ut?
nda
neyenrode
-->
<xsl:key name="kCodeByName" match="code" use="string(@key)"/>


               <xsl:template name="terminate">
                	<xsl:message terminate="yes">
                             	record is not compliant, transformation is interrupted.
                	</xsl:message>
               </xsl:template>

                        <xsl:template match="/">
                              <record>
                                  <xsl:apply-templates select="//*[local-name() = 'header']" />
                                  <metadata>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:title"/>
                                            <xsl:with-param name="targetElement" select="'dc:title'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:creator/replace(., '^(.*)\|.*$', '$1')"/>
                                            <xsl:with-param name="targetElement" select="'dc:creator'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:contributor"/>
                                            <xsl:with-param name="targetElement" select="'dc:contributor'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:description"/>
                                            <xsl:with-param name="targetElement" select="'dc:description'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:subject"/>
                                            <xsl:with-param name="targetElement" select="'dc:subject'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:publisher"/>
                                            <xsl:with-param name="targetElement" select="'dc:publisher'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:format"/>
                                            <xsl:with-param name="targetElement" select="'dc:format'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:type"/>
                                            <xsl:with-param name="targetElement" select="'dc:type'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:source"/>
                                            <xsl:with-param name="targetElement" select="'dc:source'"/>
                                    </xsl:call-template>
                                    <dc:language>
                                             <xsl:value-of select="TransformationFunction:convertString($tf, //dc:language, 'Languages')"/>
                                    </dc:language>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:rights"/>
                                            <xsl:with-param name="targetElement" select="'dc:rights'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:relation"/>
                                            <xsl:with-param name="targetElement" select="'dc:relation'"/>
                                    </xsl:call-template>
                                    <xsl:if test="not(//dc:identifier[starts-with(., 'http')])">
                                         <xsl:call-template name="terminate"/>
                                   </xsl:if>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:identifier[starts-with(., 'http')]"/>
                                            <xsl:with-param name="targetElement" select="'dc:identifier'"/>
                                    </xsl:call-template>
         <xsl:for-each select="//dc:relation">
            <xsl:if test="matches(normalize-space(.), '(info:eu-repo/grantagreement/ec/fp7/)(\d\d\d\d\d\d)(.*)', 'i')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varFP7, replace(normalize-space(.), '(info:eu-repo/grantagreement/ec/fp7/)(\d\d\d\d\d\d)(.*)', '$2', 'i'))"/>
                </oaf:projectid>
            </xsl:if>
            <xsl:if test="matches(normalize-space(.), '(info:eu-repo/grantagreement/ec/h2020/)(\d\d\d\d\d\d)(.*)', 'i')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varH2020, replace(normalize-space(.), '(info:eu-repo/grantagreement/ec/h2020/)(\d\d\d\d\d\d)(.*)', '$2', 'i'))"/>
                </oaf:projectid>
            </xsl:if>
         </xsl:for-each>


<!--
                                    <xsl:if test="not(//didl:Component/didl:Resource[@mimeType='application/pdf'])">
                                         <xsl:call-template name="terminate"/>
                                   </xsl:if>
-->

                                  <oaf:accessrights>
                                    <xsl:value-of select="TransformationFunction:convert($tf, //dc:rights, 'AccessRights')"/>
                                  </oaf:accessrights>
<!--
         <dr:CobjCategory>
                                  <xsl:value-of select="TransformationFunction:convertString($tf, //dc:type, 'TextTypologies')" />
         </dr:CobjCategory>
-->
          <dr:CobjCategory>
                                            <xsl:variable name="varCobjCategory" select="TransformationFunction:convertString($tf, //dc:type, 'TextTypologies')" />
                                            <xsl:variable name="varSuperType" select="TransformationFunction:convertString($tf, $varCobjCategory, 'SuperTypes')"/>
                                            <xsl:attribute name="type" select="$varSuperType"/>
                                            <xsl:value-of select="$varCobjCategory" />
          </dr:CobjCategory>


<!-- review status -->
<!--   -->
<xsl:variable name="varRefereedConvt" select="for $i in (//dc:type, //dc:description, //oai:setSpec) 
                                                                           return TransformationFunction:convertString($tf, normalize-space($i), 'ReviewLevels')"/>
<xsl:variable name="varRefereedIdntf" select="(//*[string(node-name(.)) = 'dc:identifier' and matches(lower-case(.), '(^|.*[\.\-_/\s\(\)%\d#])pre[\.\-_/\s\(\)%\d#]?prints?([\.\-_/\s\(\)%\d#].*)?$')][count(//dc:identifier) = 1]/'0002', //*[string(node-name(.)) = 'dc:identifier' and matches(lower-case(.), '(^|.*[\.\-_/\s\(\)%\d#])refereed([\.\-_/\s\(\)\d%\d#].*)?$')]/'0001', //*[string(node-name(.)) = 'dc:identifier' and matches(lower-case(.), '.*-peer-reviewed-(fulltext-)?article-.*')]/'0001')"/>

<xsl:variable name="varRefereedSourc" select="//*[string(node-name(.)) = ('dc:source', 'dc:publisher') and matches(lower-case(.), '^(.*\s)?pre[\s\-_]*prints?([\s\.,].*)?$')]/'0002'"/>

<xsl:variable name="varRefereedDescr" select="(//dc:description[matches(lower-case(.), '.*(this\s*book|this\s*volume|it)\s*constitutes\s*the\s*(thoroughly\s*)?refereed') or matches(lower-case(.), '.*peer[\.\-_/\s\(\)]?review\s*under\s*responsibility\s*of.*') or matches(lower-case(.), '(this|a)\s*(article|preprint)\s*(has\s*been\s*)?(peer[\-\s]*)?reviewed\s*and\s*recommended\s*by\s*peer[\-\s]*community')]/'0001', //dc:description[matches(., '^version\s*(préliminaire.*|preliminary.*|0$)')]/'0002')"/>



<xsl:variable name="varRefereedTitle" select="(//dc:title[matches(lower-case(.), '.*\[.*peer[\s\-\._]*review\s*:.*\]\s*$')]/'0001', 
                                    //dc:title[matches(lower-case(.), '.*\(\s*pre[\s\-\._]*prints?\s*\)\s*$')]/'0002')"/>



<xsl:variable name="varRefereedSubjt" select="(//dc:subject[matches(lower-case(.), '^\s*refereed\s*$')][//oaf:datasourceprefix = 'narcis______']/'0001', 
                                  //dc:subject[matches(lower-case(.), '^\s*no[nt].{0,3}refereed\s*$')][//oaf:datasourceprefix = 'narcis______']/'0002')"/>


<xsl:variable name="varRefereed" select="($varRefereedConvt, $varRefereedIdntf, $varRefereedSourc, $varRefereedDescr, $varRefereedTitle, $varRefereedSubjt)"/>
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
               <xsl:value-of select="TransformationFunction:convertString($tf, //dc:date[1], 'DateISO8601')"/>
         </oaf:dateAccepted>


     <xsl:if test="//dc:relation[starts-with(., 'http')] and //dc:rights[.='info:eu-repo/semantics/openAccess']">
         <oaf:fulltext>
            <xsl:value-of select="//dc:relation[starts-with(., 'http')]"/>
         </oaf:fulltext>         
     </xsl:if>
         <oaf:hostedBy>
            <xsl:attribute name="name">
               <xsl:value-of select="key('kCodeByName', $repoCode, $vCodes)/@name"/>
            </xsl:attribute>
            <xsl:attribute name="id">
               <xsl:value-of select="key('kCodeByName', $repoCode, $vCodes)/@id"/>
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

 <!-- ID recognition incomplete -->
            <xsl:variable name="varKnownFileEndings" select="('.bmp', '.doc', '.docx', '.epub', '.flv', '.jpeg', '.jpg', '.m4v', '.mp4', '.mpg', '.odp', '.pdf', '.png', '.ppt', '.tiv', '.txt', '.xls', '.xlsx', '.zip')" />

            <!-- regular expression for DOIs reduced here - letters like less-than and quotation marks don't work in matches, use identiferExtract when enabled -->
            <xsl:variable name="varIdDoi" select="distinct-values((//dc:identifier[starts-with(., '10.')][matches(., '(10[.][0-9]{4,}[^\s/>]*/[^\s>]+)')], //dc:identifier[starts-with(., 'http') and (contains(., '://dx.doi.org/10.') or contains(., '://doi.org/10.'))]/substring-after(., 'doi.org/'), //dc:identifier[starts-with(lower-case(.), 'doi:10.')]/substring-after(lower-case(.), 'doi:')))" />
         <xsl:for-each select="$varIdDoi">
         <oaf:identifier>
            <xsl:attribute name="identifierType" select="'doi'"/>
            <xsl:value-of select="."/>
         </oaf:identifier>
         </xsl:for-each>

         <xsl:variable name="varIdHdl" select="distinct-values(//dc:identifier[starts-with(., 'http') and contains(., '://hdl.handle.net/')]/substring-after(., 'hdl.handle.net/'))" />
         <xsl:for-each select="$varIdHdl">
         <oaf:identifier>
            <xsl:attribute name="identifierType" select="'handle'"/>
            <xsl:value-of select="."/>
         </oaf:identifier>
         </xsl:for-each>

         <xsl:variable name="varIdUrn" select="distinct-values(//dc:identifier[starts-with(., 'urn:nbn:nl:') or starts-with(., 'URN:NBN:NL:')])" />
         <xsl:for-each select="$varIdUrn">
         <oaf:identifier>
            <xsl:attribute name="identifierType" select="'urn'"/>
            <xsl:value-of select="."/>
         </oaf:identifier>
         </xsl:for-each>

            <!-- the 2 comparison orders needed to work also for URL encoded baseURLs or item URLs -->
            <xsl:variable name="varOrigBaseUrl" select="//*[local-name() = 'about']/*[local-name() = 'provenance']//*[local-name() = 'originDescription' and not(./*[local-name() = 'originDescription'])]/*[local-name() = 'baseURL']" />
            <xsl:variable name="varIdLdpg" select="distinct-values(//dc:identifier[(contains(substring-after(., '://'), '/') and contains($varOrigBaseUrl, substring-before(substring-after(., '://'), '/'))) or (contains(substring-after(., '://'), ':') and contains($varOrigBaseUrl, substring-before(substring-after(., '://'), ':')))][not(replace(lower-case(.), '.*(\.[a-z]*)$', '$1') = $varKnownFileEndings)])" />
         <xsl:for-each select="$varIdLdpg">
         <oaf:identifier>
            <xsl:attribute name="identifierType" select="'landingPage'"/>
            <xsl:value-of select="."/>
         </oaf:identifier>
         </xsl:for-each>

         <xsl:variable name="varIdUrl" select="distinct-values(//dc:identifier[starts-with(., 'http')][not(contains(., '://dx.doi.org/') or contains(., '://doi.org/') or contains(., '://hdl.handle.net/'))][count(index-of($varIdLdpg, .)) = 0])" />
         <xsl:for-each select="$varIdUrl">
         <oaf:identifier>
            <xsl:attribute name="identifierType" select="'url'"/>
            <xsl:value-of select="."/>
         </oaf:identifier>
         </xsl:for-each>

         <oaf:identifier>
            <xsl:attribute name="identifierType" select="'oai-original'"/>
            <xsl:value-of select="//*[local-name() = 'about']/*[local-name() = 'provenance']//*[local-name() = 'originDescription' and not(./*[local-name() = 'originDescription'])]/*[local-name() = 'identifier']"/>
         </oaf:identifier>

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


                      <xsl:template match="//*[local-name() = 'header']">
                         <xsl:copy>
                            <xsl:apply-templates  select="node()|@*"/>
                             <xsl:element name="dr:dateOfTransformation">
                                 <xsl:value-of select="$transDate"/>
                             </xsl:element>
                         </xsl:copy>
                        </xsl:template>

<!--
                       <xsl:template match="dri:objIdentifier">
                         <xsl:variable name="objIdentifier" select="substring-after(//*[local-name() = 'header']/*[local-name()='recordIdentifier'], ':')"/>
                         <xsl:variable name="nsPrefix" select="key('kCodeByName', $repoCode, $vCodes)/@value"/>
                         <xsl:if test="string-length($nsPrefix) = 0">
                                         <xsl:call-template name="terminate"/>
                         </xsl:if>
                        <xsl:copy>
                        <xsl:value-of select="concat( $nsPrefix , '::', transformExt:md5Hex(string($objIdentifier)))"/>
                        </xsl:copy>
                       </xsl:template>
-->
                       <xsl:template match="node()|@*">
                            <xsl:copy>
                                 <xsl:apply-templates select="node()|@*"/>
                            </xsl:copy>
                       </xsl:template>
                    </xsl:stylesheet>

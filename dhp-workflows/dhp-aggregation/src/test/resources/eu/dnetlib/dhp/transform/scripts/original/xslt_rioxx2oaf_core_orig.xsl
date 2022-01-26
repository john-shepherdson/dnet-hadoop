<!-- production ; 2021-10-11 -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.1" 
                             xmlns:dri="http://www.driver-repository.eu/namespace/dri" 
                             xmlns:dc="http://purl.org/dc/elements/1.1/" 
                             xmlns:rioxxterms="http://docs.rioxx.net/schema/v2.0/rioxxterms/" 
                             xmlns:oaf="http://namespace.openaire.eu/oaf" 
                             xmlns:dr="http://www.driver-repository.eu/namespace/dr" 
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

  <xsl:param name="repoCode" select="replace(//*[local-name() = 'header']/*[local-name()='recordIdentifier'], '^(.*):.*$', '$1')"/>

  <xsl:param name="index" select="0"/>
  <xsl:param name="transDate" select="current-dateTime()"/>
  <xsl:variable name="tf" select="TransformationFunction:getInstance()"/>



<xsl:variable name="vCodes">
  <codes>
   <code key="oai:archive.lstmed.ac.uk" value="od______2698"   id="opendoar____:2698:" name="LSTM Online Archive"/>
   <code key="oai:arro.anglia.ac.uk" value="od______1850"   id="opendoar____::1850" name="Anglia Ruskin Research Online"/>
   <code key="oai:centaur.reading.ac.uk" value="od______1731"    id="opendoar____::1731" name="Central Archive at the University of Reading"/>
   <code key="oai:clok.uclan.ac.uk" value="od______1882"   id="opendoar____::1882" name="Central Lancashire Online Knowledge"/>
   <code key="oai:collections.crest.ac.uk" value="od______1603"   id="opendoar____::1603" name="Glyndŵr University Research Online"/>
   <code key="oai:create.canterbury.ac.uk" value="od______2172"   id="opendoar____::2172" name="Canterbury Research and Theses Environment"/>
   <code key="oai:eprints.bbk.ac.uk.oai2" value="od________30"   id="opendoar____::30" name="Birkbeck Institutional Research Online"/>
   <code key="oai:eprints.bournemouth.ac.uk" value="od______1076"   id="opendoar____::1076" name="Bournemouth University Research Online"/> 
   <code key="oai:eprints.chi.ac.uk" value="od______2701" id="opendoar____::2701" name="University of Chichester ChiPrints Repository"/>
   <code key="oai:eprints.esc.cam.ac.uk" value="od______3280"   id="opendoar____::3280" name="ESC Publications - Cambridge Univesity"/>
   <code key="oai:eprints.gla.ac.uk" value="od_______162"  id="opendoar____::162" name="Enlighten"/>
   <code key="oai:eprints.glos.ac.uk" value="od______3160"   id="opendoar____::3160" name="University of Gloucestershire Research Repository"/>
   <code key="oai:eprints.gold.ac.uk" value="od______758"  id="opendoar____::758" name="Goldsmiths Research Online"/>
   <code key="oai:eprints.hud.ac.uk" value="od______1012"   id="opendoar____::1012" name="University of Huddersfield Repository"/>
   <code key="oai:eprints.keele.ac.uk" value="od______2263"   id="opendoar____::2263" name="Keele Research Repository"/>
   <code key="oai:eprints.kingston.ac.uk" value="od______1221"   id="opendoar____::1221" name="Kingston University Research Repository"/>
   <code key="oai:eprints.leedsbeckett.ac.uk" value="od______1551"   id="opendoar____::1551" name="Leeds Beckett Repository"/>
   <code key="oai:eprints.lse.ac.uk" value="od_______206"   id="opendoar____::206" name="LSE Research Online"/>
   <code key="oai:eprints.mdx.ac.uk" value="od_______213"   id="opendoar____::213" name="Middlesex University Research Repository"/>
   <code key="oai:eprints.nottingham.ac.uk" value="od_______226"   id="opendoar____::226" name="Nottingham ePrints"/>
   <code key="oai:eprints.soas.ac.uk" value="od_______285"  id="opendoar____::285" name="SOAS Research Online"/>
   <code key="oai:eprints.soton.ac.uk" value="od_______348"   id="opendoar____::348" name="e-Prints Soton"/>
   <code key="oai:eprints.staffs.ac.uk" value="od______2881"  id="opendoar____::2881" name="STORE - Staffordshire Online Repository"/>
   <code key="oai:eprints.uwe.ac.uk" value="od______1749"   id="opendoar____::1749" name="UWE Research Repository"/>
   <code key="oai:eprints.whiterose.ac.uk" value="od_______373"   id="opendoar____::373" name="White Rose Research Online"/>
   <code key="oai:eresearch.qmu.ac.uk" value="od______1228"   id="opendoar____::1228" name="Queen Margaret University eResearch"/>
   <code key="oai:gala.gre.ac.uk" value="od______1756"   id="opendoar____::1756" name="Greenwich Academic Literature Archive"/>
   <code key="oai:http://orca.cf.ac.uk" value="od________55"   id="opendoar____::55" name="Online Research @ Cardiff"/>
   <code key="oai:insight.cumbria.ac.uk" value="od______1675"   id="opendoar____::1675" name="Insight - University of Cumbria"/>
   <code key="oai:irep.ntu.ac.uk" value="od______1226"   id="opendoar____::1226" name="Nottingham Trent Institutional Repository (IRep)"/>
   <code key="oai:kar.kent.ac.uk" value="od______1328"   id="opendoar____::1328" name="Kent Academic Repository"/>
   <code key="oai:kultur.ucreative.ac.uk" value="od______1925"   id="opendoar____::1925" name="UCA Research Online"/>
   <code key="oai:lbsresearch.london.edu" value="od______3652"   id="opendoar____::3652" name="LBS Research Online"/>
   <code key="oai:livrepository.liverpool.ac.uk" value="od______1292"   id="opendoar____::1292" name="University of Liverpool Repository"/>
   <code key="oai:napier-surface.worktribe.com" value="od______1334"  id="opendoar____::1334" name="Repository@Napier"/>
   <code key="oai:nectar.northampton.ac.uk" value="od______1251"  id="opendoar____::1251" name="NECTAR"/>
   <code key="oai:nottingham-repository.worktribe.com" value="od______4162"   id="opendoar____::4162" name="Nottingham Research Repository"/>
   <code key="oai:nrl.northumbria.ac.uk" value="od______1448"   id="opendoar____::1448" name="Northumbria Research Link"/>
   <code key="oai:openaccess.city.ac.uk" value="od______2262"   id="opendoar____::2262" name="City Research Online"/>
   <code key="oai:openaccess.sgul.ac.uk" value="od______2556"  id="opendoar____::2556" name="St George's Online Research Archive"/>
   <code key="oai:open.ac.uk.OAI2" value="od_______300"  id="opendoar____::300" name="Open Research Online"/>
   <code key="oai:openresearch.lsbu.ac.uk" value="od______3564"   id="opendoar____:3564:" name="LSBU Research Open"/>
   <code key="oai:plymsea.ac.uk" value="od______3572"   id="opendoar____::3572" name="Plymouth Marine Science Electronic Archive (PlyMEA)"/>
   <code key="oai:publications.aston.ac.uk" value="od______1552"   id="opendoar____::1552" name="Aston Publications Explorer"/>
   <code key="oai:publications.heythrop.ac.uk" value="od______2697"   id="opendoar____::2697" name="Heythrop College Publications"/>
   <code key="oai:radar.gsa.ac.uk" value="od______2695"   id="opendoar____::2695" name="Research Art Design Architecture Repository"/>
   <code key="oai:ray.yorksj.ac.uk" value="od______1391"   id="opendoar____::1391" name="York St John University Repository"/>
   <code key="oai:repository.falmouth.ac.uk" value="od______3261"   id="opendoar____::3261" name="Falmouth University Research Repository (FURR)"/>
   <code key="oai:repository.londonmet.ac.uk" value="od______3367"   id="opendoar____::3367" name="London Met Repository"/>
   <code key="oai:repository.uwl.ac.uk" value="od______2700"   id="opendoar____::2700" name="UWL Repository"/>
   <code key="oai:repository.winchester.ac.uk" value="od______3794"   id="opendoar____::3794" name="Winchester Research Repository"/>
   <code key="oai:researchonline.bishopg.ac.uk" value="od______3650"   id="opendoar____::3650" name="BG Research Online"/>
   <code key="oai:researchonline.ljmu.ac.uk" value="od______3107"   id="opendoar____::3107" name="LJMU Research Online"/>
   <code key="oai:researchonline.lshtm.ac.uk" value="od______2377"   id="opendoar____::2377" name="LSHTM Research Online"/>
   <code key="oai:researchonline.rcm.ac.uk" value="rcm_ac_uk___"   id="opendoar____::5143" name="Royal College of Music Research Online"/>
   <code key="oai:researchonline.rvc.ac.uk" value="od______3495"   id="opendoar____::3495" name="RVC Research Online"/>
   <code key="oai:researchopen.lsbu.ac.uk" value="od______3564"   id="opendoar____:3564:" name="LSBU Research Open"/>
   <code key="oai:researchspace.bathspa.ac.uk" value="od______2694" id="opendoar____::2694" name="ResearchSPace - Bath Spa University"/>
   <code key="oai:research.stmarys.ac.uk" value="od______2478"   id="opendoar____::2478" name="St Mary's University Open Research Archive"/>
   <code key="oai:roar.uel.ac.uk" value="od______1488"  id="opendoar____::1488" name="ROAR at University of East London"/>
   <code key="oai:sas-space.sas.ac.uk" value="od_______907"   id="opendoar____::907" name="SAS-SPACE"/>
   <code key="oai:shura.shu.ac.uk" value="od_______942"   id="opendoar____::942" name="Sheffield Hallam University Research Archive"/>
   <code key="oai:sro.sussex.ac.uk" value="od______2384"   id="opendoar____::2384" name="Sussex Research Online"/>
   <code key="oai:sro.sussex.ac.uk" value="od______897"   id="opendoar____::897" name="Sussex Research Online"/>
   <code key="oai:strathprints.strath.ac.uk" value="od______1181" id="opendoar____::1181" name="Strathprints"/>
   <code key="oai:sure.sunderland.ac.uk" value="od______2408"   id="opendoar____::2408" name="Sunderland University Institutional Repository"/>
   <code key="oai:usir.salford.ac.uk" value="od_______991"   id="opendoar____::991" name="University of Salford Institutional repository"/>
   <code key="oai:westminsterresearch.wmin.ac.uk" value="od_______936"  id="opendoar____::936" name="WestminsterResearch"/>
   <code key="oai:wrap.eprints.org" value="od______1177"   id="opendoar____::1177" name="University of Worcester Research and Publications"/>
   <code key="oai:wrap.warwick.ac.uk" value="od______1288"  id="opendoar____::1288" name="Warwick Research Archives Portal Repository"/>
  </codes>
 </xsl:variable>
<!-- 
status core-id opendoar-id repo name
na 8 30 Birkbeck Institutional Research Online
na 9 1076 Bournemouth University Research Online
ok 17    1731    Central Archive at the University of Reading
na 18  1882  Central Lancashire Online Knowledge
na 34  348 e-Prints Soton
ok 42    162        Enlighten
ok  49 1603 Glynd\u0175r University Research Online
ok  50 758 Goldsmiths Research Online
ok ? 51 1756 Greenwich Academic Literature Archive
na 53  1226  Nottingham Trent Institutional Repository (IRep)
na 57  1328  Kent Academic Repository
na 62 1551 Leeds Beckett Repository
ok  65 3107 LJMU Research Online  
ok 67    206        LSE Research Online
ok 78    1251    NECTAR                                      http://nectar.northampton.ac.uk/cgi/oai2?verb=Identify
ok 83 55 Online Research @ Cardiff
ok 86    300        Open Research Online
ok 92    1334    Repository@Napier
ok 96    1488    ROAR at University of East London
ok 103    285        SOAS Research Online
ok 109    2384    Sussex Research Online
ok 117 1925 "UCA Research Online"
ok 126 1012 University of Huddersfield Repository
ok 130    991        University of Salford Institutional repository  oai:usir.salford.ac.uk
ok 134 1749 UWE Research Repository
ok 136    ​1288    Warwick Research Archives Portal Repository
ok 138    936        WestminsterResearch
ok 140    0        White Rose Research Online
na 147  2377 LSHTM Research Online
ok 148 2697 Heythrop College Publications
ok 162 1448 Northumbria Research Link
ok 175 2478 St Mary\u0027s University Open Research Archive
ok 253    2556    St George's Online Research Archive
ok 620    2881    STORE - Staffordshire Online Repository
ok 683    0    ! 1391    York St John University Repository              http://ray.yorksj.ac.uk/cgi/oai2
ok 686    0    !2694        ResearchSPace - Bath Spa University              http://researchspace.bathspa.ac.uk/cgi/oai2
ok ? 694    0        Winchester Research Repository                  http://repository.winchester.ac.uk/cgi/oai2 oai:repository.winchester.ac.uk
? no records so far 170    2134    LSE Theses Online                              http://etheses.lse.ac.uk/cgi/oai2
no longer existant:    code key="oai:eprints.bucks.ac.uk" value="od______1382"   id="opendoar____::1382" name="Bucks Knowledge Archive"
   <code key="oai:eprints.lse.ac.uk" value="od______2134"   id="opendoar____::2134" name="LSE Theses Online"/>
-->
<xsl:key name="kCodeByName" match="code" use="string(@key)"/>


               <xsl:template name="terminate">
                	<xsl:message terminate="yes">
                             	record is not compliant, transformation is interrupted.
                	</xsl:message>
               </xsl:template>

                        <xsl:template match="/">
                                    <xsl:if test="not(//dc:identifier[starts-with(., 'http')])">
                                         <xsl:call-template name="terminate"/>
                                   </xsl:if>
                      <xsl:if test="not(//*[local-name()='project'] or //*[local-name()='free_to_read'])">
                            <xsl:call-template name="terminate"/>
                      </xsl:if>
                      <xsl:if test="not(//dc:title or //dc:title[string-length(.) eq 0])">
                            <xsl:call-template name="terminate"/>
                      </xsl:if>

                              <record>
                                  <xsl:apply-templates select="//*[local-name() = 'header']" />
                                  <metadata>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:title"/>
                                            <xsl:with-param name="targetElement" select="'dc:title'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="authors">
                                            <xsl:with-param name="sourceElement" select="//*[local-name() = 'author'][normalize-space(.) != ',']"/>
                                    </xsl:call-template>
<!--
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='author']"/>
                                            <xsl:with-param name="targetElement" select="'dc:creator'"/>
                                    </xsl:call-template>
-->
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='contributor']"/>
                                            <xsl:with-param name="targetElement" select="'dc:contributor'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:description"/>
                                            <xsl:with-param name="targetElement" select="'dc:description'"/>
                                    </xsl:call-template>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:format"/>
                                            <xsl:with-param name="targetElement" select="'dc:format'"/>
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
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//dc:identifier[starts-with(., 'http')]"/>
                                            <xsl:with-param name="targetElement" select="'dc:identifier'"/>
                                    </xsl:call-template>
                                    <xsl:if test="//*[local-name()='version_of_record'][starts-with(., 'http://dx.doi.org/')]">
                                     <xsl:element name="oaf:identifier"> 
                                          <xsl:attribute name="identifierType">
                                              <xsl:text>doi</xsl:text>
                                          </xsl:attribute>
                                            <xsl:value-of select="substring-after(//*[local-name()='version_of_record'], 'http://dx.doi.org/')"/>
                                        </xsl:element>
                                    </xsl:if>

         <xsl:for-each select="//*[local-name()='project']">
            <xsl:if test="(contains(@funder_name, 'EU FP7') or starts-with(@funder_name, 'Europe') or contains(@funder_id, '10.13039/501100000780') or contains(@funder_id, '10.13039/501100004963') ) and not(contains(., '531245')) and not(contains(lower-case(.), 'lifelong learning programme')) and matches(normalize-space(.), '^.*(\d\d\d\d\d\d).*$', 'i')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varFP7, replace(normalize-space(.), '^.*(\d\d\d\d\d\d).*$', '$1', 'i'))"/>
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
                                   <xsl:choose>
                                            <xsl:when test="//*[local-name()='free_to_read']" >
                                  <oaf:accessrights>
                                      <xsl:text>OPEN</xsl:text>
                                  </oaf:accessrights>
                                            </xsl:when>
                                            <xsl:otherwise>
                                  <oaf:accessrights>
                                      <xsl:text>CLOSED</xsl:text>
                                  </oaf:accessrights>
                                            </xsl:otherwise>
                                            </xsl:choose>
                                    <xsl:call-template name="allElements">
                                            <xsl:with-param name="sourceElement" select="//*[local-name()='license_ref']"/>
                                            <xsl:with-param name="targetElement" select="'oaf:license'"/>
                                    </xsl:call-template>
                                   <dr:CobjCategory>
                                            <xsl:variable name="varCobjCategory" select="TransformationFunction:convertString($tf, //rioxxterms:type, 'TextTypologies')" />
                                            <xsl:variable name="varSuperType" select="TransformationFunction:convertString($tf, $varCobjCategory, 'SuperTypes')"/>
                                            <xsl:attribute name="type" select="$varSuperType"/>
                                            <xsl:value-of select="$varCobjCategory" />
                                   </dr:CobjCategory>
<!--
                                  <dr:CobjCategory>
                                  <xsl:value-of 
                 select="TransformationFunction:convertString($tf, //rioxxterms:type, 'TextTypologies')" />
         </dr:CobjCategory>
                 select="TransformationFunction:convertString($tf, //*[local-name()='type'], 'TextTypologies')" />
-->
         <oaf:dateAccepted>
               <xsl:value-of select="TransformationFunction:convertString($tf, //*[local-name()='publication_date'], 'DateISO8601')"/>
         </oaf:dateAccepted>

          <oaf:fulltext>
             <xsl:value-of select="//dc:identifier[ends-with(., '.pdf')] | //dc:relation[ends-with(., '.pdf')]"/>
          </oaf:fulltext>

         <oaf:hostedBy>
            <xsl:attribute name="name">
               <xsl:value-of select="key('kCodeByName', $repoCode, $vCodes)/@name"/>
            </xsl:attribute>
            <xsl:attribute name="id">
               <xsl:value-of select="key('kCodeByName', $repoCode, $vCodes)/@id"/>
            </xsl:attribute>
         </oaf:hostedBy>

<!--
         <oaf:hostedBy>
            <xsl:attribute name="name">
               <xsl:value-of select="$varOfficialName"/>
            </xsl:attribute>
            <xsl:attribute name="id">
               <xsl:value-of select="$varDataSourceId"/>
            </xsl:attribute>
         </oaf:hostedBy>
-->

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

          <xsl:template name="authors">
             <xsl:param name="sourceElement"/>
             <xsl:for-each select="$sourceElement">
                <xsl:element name="dc:creator">
                    <xsl:if test="starts-with(./@id, 'http://orcid.org')">
                         <xsl:attribute name="nameIdentifierScheme">
                             <xsl:text>ORCID</xsl:text>
                         </xsl:attribute>
                         <xsl:attribute name="schemeURI">
                             <xsl:text>http://orcid.org/</xsl:text>
                         </xsl:attribute> 
                         <xsl:attribute name="nameIdentifier">
                             <xsl:value-of select="substring-after(./@id, 'http://orcid.org/')"/>
                         </xsl:attribute> 
                    </xsl:if>
                    <xsl:value-of select="."/>
                </xsl:element>
             </xsl:for-each>              
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
<xsl:stylesheet
        version="2.0"
        xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
        xmlns:oai="http://www.openarchives.org/OAI/2.0/"
        xmlns:oaf="http://namespace.openaire.eu/oaf"
        xmlns:vocabulary="http://eu/dnetlib/transform/clean"
        xmlns:dateCleaner="http://eu/dnetlib/transform/dateISO"
        xmlns:dr="http://www.driver-repository.eu/namespace/dr"
        exclude-result-prefixes="xsl vocabulary dateCleaner">
    <xsl:param name="varOfficialName"/>
    <xsl:param name="varDsType"/>
    <xsl:param name="varDataSourceId"/>

    <xsl:param name="varFP7" select="'corda_______::'"/>
    <xsl:param name="varH2020" select="'corda__h2020::'"/>
    <xsl:param name="varAKA" select="'aka_________::'"/>
    <xsl:param name="varARC" select="'arc_________::'"/>
    <xsl:param name="varCONICYT" select="'conicytf____::'"/>
    <xsl:param name="varDFG" select="'dfgf________::'"/>
    <xsl:param name="varFCT" select="'fct_________::'"/>
    <xsl:param name="varFWF" select="'fwf_________::'"/>
    <xsl:param name="varHRZZ" select="'irb_hr______::'"/>
    <xsl:param name="varMESTD" select="'mestd_______::'"/>
    <xsl:param name="varMZOS" select="'irb_hr______::'"/>
    <xsl:param name="varNHMRC" select="'nhmrc_______::'"/>
    <xsl:param name="varNIH" select="'nih_________::'"/>
    <xsl:param name="varNSF" select="'nsf_________::'"/>
    <xsl:param name="varNWO" select="'nwo_________::'"/>
    <xsl:param name="varRCUK" select="'rcuk________::'"/>
    <xsl:param name="varSFI" select="'sfi_________::'"/>
    <xsl:param name="varSGOV"
               select="'sgov________::'"/>     <!-- SGOV awaiting DOI from Pilar, found project ids not in CSV list? -->
    <xsl:param name="varSNSF" select="'snsf________::'"/>
    <xsl:param name="varTARA" select="'taraexp_____::'"/>     <!-- TARA awaiting DOI from André -->
    <xsl:param name="varTUBITAK" select="'tubitakf____::'"/>
    <xsl:param name="varWT" select="'wt__________::'"/>

    <xsl:param name="index" select="0"/>
    <xsl:param name="transDate" select="current-dateTime()"/>


    <xsl:template match="/">
        <xsl:variable name="datasourcePrefix" select="normalize-space(//oaf:datasourceprefix)"/>
        <xsl:call-template name="validRecord"/>
    </xsl:template>


    <xsl:template name="validRecord">
        <record>
            <xsl:apply-templates select="//*[local-name() = 'header']"/>

            <metadata>
                <xsl:apply-templates select="//*[local-name() = 'metadata']//*[local-name() = 'resource']"/>

                <xsl:if test="//*[local-name()='date']/@dateType='Available'">
                    <xsl:variable name='varEmbargoEndDate'
                                  select="dateCleaner:dateISO(normalize-space(//*[local-name()='date'][@dateType='Available']))"/>
                    <xsl:choose>
                        <xsl:when test="string-length($varEmbargoEndDate) > 0">
                            <oaf:embargoenddate>
                                <xsl:value-of select="$varEmbargoEndDate"/>
                            </oaf:embargoenddate>
                        </xsl:when>
                        <xsl:otherwise>
                            <oaf:skip>
                                <invalid/>
                            </oaf:skip>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:if>

                <dr:CobjCategory>
                    <xsl:variable name="varCobjCategory"
                                  select="vocabulary:clean( //*[local-name()='resourceType']/@resourceTypeGeneral, 'dnet:publication_resource')"/>
                    <xsl:variable name="varSuperType"
                                  select="vocabulary:clean( $varCobjCategory, 'dnet:result_typologies')"/>

                    <xsl:attribute name="type">
                        <xsl:value-of select="$varSuperType"/>
                    </xsl:attribute>
                    <xsl:value-of select="$varCobjCategory"/>
                </dr:CobjCategory>

                <!-- review status -->
                <!-- Zenodo  -->
                <xsl:variable name="varRefereedConvt" select="for $i in (//*[local-name()='resourceType']/(., @resourceTypeGeneral), //*[local-name()='version'])
        return vocabulary:clean(normalize-space($i), 'dnet:review_levels')"/>
                <xsl:variable name="varRefereedIdntf"
                              select="//*[local-name()=('identifier', 'alternateIdentifier')][matches(lower-case(.), '.*([\s\-\.\\_/:]|%[2-7][0-9A-F])pre([\s\-\.\\_/:]|%[2-7][0-9A-F])?prints?([\s\-\.\\_/:%].*|$)')]/'0002' "/>
                <xsl:variable name="varRefereedReltn"
                              select="//*[local-name()='relatedIdentifier'][./@relationType/lower-case(.)='isreviewedby']/'0001' "/>
                <xsl:variable name="varRefereedVersn" select="(//*[local-name()='version'][matches(lower-case(.), '.*peer[\s\-\.\\_/:%]?reviewed.*')]/'0001',
        //*[local-name()='version'][matches(normalize-space(lower-case(.)), '^(v|vs|version|rel|release)?[\s\.\-_]*0$')]/'0002',
        //*[local-name()='version'][matches(lower-case(.), '(^|[\s\-\.\\_/:%].*)(beta|draft|trial|test)([\s\-\.\\_/:%].*|$)')]/'0002',
        //*[local-name()='version'][matches(lower-case(.), '.*submi(tted|ssion|ttal).*')]/'0002') "/>
                <xsl:variable name="varRefereedOther" select="(//*[local-name()='publisher'][matches(lower-case(.), '.*[\s\-\.\\_/:%]pre[\s\-\.\\_/:%]?prints?([\s\-\.\\_/:%].*|$)')]/'0002',
        //*[local-name()='description'][matches(lower-case(.), '^peer[\s\-\.\\_/:%]?reviewed$')]/'0001',
        //*[local-name()='description'][matches(lower-case(.), '^pre[\s\-\.\\_/:%]?prints?$')]/'0002') "/>
                <xsl:variable name="varRefereed"
                              select="($varRefereedConvt, $varRefereedIdntf, $varRefereedReltn, $varRefereedVersn, $varRefereedOther)"/>
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
                    <xsl:value-of
                            select="dateCleaner:dateISO(normalize-space(//*[local-name()='publicationYear']))"/>
                </oaf:dateAccepted>
                <xsl:choose>

                    <xsl:when
                            test="//*[local-name() = 'rights'][starts-with(normalize-space(.), 'info:eu-repo/semantics')]">
                        <oaf:accessrights>
                            <xsl:value-of
                                    select="vocabulary:clean( //*[local-name() = 'rights'][starts-with(normalize-space(.), 'info:eu-repo/semantics')], 'dnet:access_modes')"/>
                        </oaf:accessrights>
                    </xsl:when>
                    <xsl:when
                            test="//*[local-name() = 'rights']/@rightsURI[starts-with(normalize-space(.), 'info:eu-repo/semantics')]">
                        <oaf:accessrights>
                            <xsl:value-of
                                    select="vocabulary:clean( //*[local-name() = 'rights']/@rightsURI[starts-with(normalize-space(.), 'info:eu-repo/semantics')], 'dnet:access_modes')"/>
                        </oaf:accessrights>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:choose>
                            <xsl:when
                                    test="//*[local-name() = 'rights'][starts-with(normalize-space(.), 'http://creativecommons.org')]">
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
                    </xsl:otherwise>
                </xsl:choose>

                <oaf:language>
                    <xsl:value-of
                            select="vocabulary:clean(//*[local-name()='language'], 'dnet:languages')"/>
                </oaf:language>


                <xsl:for-each
                        select="//*[local-name()='nameIdentifier'][contains(., 'info:eu-repo/grantAgreement/')], //*[local-name()='fundingReference']/*[local-name()='awardNumber']">
                    <xsl:choose>

                        <xsl:when
                                test="matches(normalize-space(.), '(info:eu-repo/grantagreement/ec/fp7/)(\d\d\d\d\d\d)(.*)', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/100011102']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varFP7, replace(normalize-space(.), '(info:eu-repo/grantagreement/ec/fp7/)(\d\d\d\d\d\d)(.*)', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), '(info:eu-repo/grantagreement/ec/h2020/)(\d\d\d\d\d\d)(.*)', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/501100000780']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varH2020, replace(normalize-space(.), '(info:eu-repo/grantagreement/ec/h2020/)(\d\d\d\d\d\d)(.*)', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), 'info:eu-repo/grantagreement/aka/.*', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/501100002341']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varAKA, replace(normalize-space(.), '(info:eu-repo/grantagreement/aka/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), 'info:eu-repo/grantagreement/arc/.*', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/501100000923']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varARC, replace(normalize-space(.), '(info:eu-repo/grantagreement/arc/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), 'info:eu-repo/grantagreement/conicyt/.*', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/501100002848']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varCONICYT, replace(normalize-space(.), '(info:eu-repo/grantagreement/conicyt/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), 'info:eu-repo/grantagreement/dfg/.*', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/501100001659']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varDFG, replace(normalize-space(.), '(info:eu-repo/grantagreement/dfg/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), 'info:eu-repo/grantagreement/fct/.*', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/501100001871']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varFCT, replace(normalize-space(.), '(info:eu-repo/grantagreement/fct/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), 'info:eu-repo/grantagreement/fwf/.*', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/501100002428']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varFWF, replace(normalize-space(.), '(info:eu-repo/grantagreement/fwf/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), 'info:eu-repo/grantagreement/hrzz/.*', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/501100004488']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varHRZZ, replace(normalize-space(.), '(info:eu-repo/grantagreement/hrzz/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when test="matches(normalize-space(.), 'info:eu-repo/grantagreement/mestd/.*', 'i')">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varMESTD, replace(normalize-space(.), '(info:eu-repo/grantagreement/mestd/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when test="matches(normalize-space(.), 'info:eu-repo/grantagreement/mzos/.*', 'i')">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varMZOS, replace(normalize-space(.), '(info:eu-repo/grantagreement/mzos/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), 'info:eu-repo/grantagreement/nhmrc/.*', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/501100000925']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varNHMRC, replace(normalize-space(.), '(info:eu-repo/grantagreement/nhmrc/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), 'info:eu-repo/grantagreement/nih/.*', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/100000002']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varNIH, replace(normalize-space(.), '(info:eu-repo/grantagreement/nih/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), 'info:eu-repo/grantagreement/nsf/.*', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/100000001']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varNSF, replace(normalize-space(.), '(info:eu-repo/grantagreement/nsf/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), 'info:eu-repo/grantagreement/nwo/.*', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/501100003246']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varNWO, replace(normalize-space(.), '(info:eu-repo/grantagreement/nwo/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), 'info:eu-repo/grantagreement/rcuk/.*', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/501100000690']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varRCUK, replace(normalize-space(.), '(info:eu-repo/grantagreement/rcuk/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), 'info:eu-repo/grantagreement/sfi/.*', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/501100001602']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varSFI, replace(normalize-space(.), '(info:eu-repo/grantagreement/sfi/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when test="matches(normalize-space(.), 'info:eu-repo/grantagreement/sgov/.*', 'i')">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varSGOV, replace(normalize-space(.), '(info:eu-repo/grantagreement/sgov/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), 'info:eu-repo/grantagreement/snsf/.*', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/501100001711']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varSNSF, replace(normalize-space(.), '(info:eu-repo/grantagreement/snsf/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when test="matches(normalize-space(.), 'info:eu-repo/grantagreement/tara/.*', 'i')">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varTARA, replace(normalize-space(.), '(info:eu-repo/grantagreement/tara/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), 'info:eu-repo/grantagreement/tubitak/.*', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/501100004410']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varTUBITAK, replace(normalize-space(.), '(info:eu-repo/grantagreement/tubitak/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>
                        <xsl:when
                                test="matches(normalize-space(.), 'info:eu-repo/grantagreement/wt/.*', 'i') or ../*[local-name() = 'funderIdentifier' and . = '10.13039/100004440']">
                            <oaf:projectid>
                                <xsl:value-of
                                        select="concat($varWT, replace(normalize-space(.), '(info:eu-repo/grantagreement/wt/.*?/)([^/]*)(/.*)?', '$2', 'i'))"/>
                            </oaf:projectid>
                        </xsl:when>

                    </xsl:choose>
                </xsl:for-each>

                <xsl:for-each select="//*[local-name()='relatedIdentifier']">
                    <xsl:if
                            test="starts-with(./text(), 'https://zenodo.org/communities/')">
                        <oaf:concept>
                            <xsl:attribute name="id">
                                <xsl:value-of select="./text()"/>
                            </xsl:attribute>
                        </oaf:concept>
                    </xsl:if>
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
            </metadata>
            <xsl:copy-of select="//*[local-name() = 'about']"/>
        </record>
    </xsl:template>

    <xsl:template match="node()|@*">
        <xsl:copy>
            <xsl:apply-templates select="node()|@*"/>
        </xsl:copy>
    </xsl:template>

    <xsl:template match="//*[local-name() = 'metadata']//*[local-name() = 'resource']">
        <xsl:copy>
            <xsl:apply-templates select="node()|@*"/>
        </xsl:copy>
    </xsl:template>

    <xsl:template match="//*[local-name() = 'resource']/*[local-name()='alternateIdentifiers']">
        <xsl:element name="alternateIdentifiers" namespace="http://www.openarchives.org/OAI/2.0/">

            <xsl:copy-of select="./*"/>

            <xsl:if test="//*[local-name() = 'resource']/*[local-name()='identifier'][@identifierType='Handle']">
                <xsl:element name="alternateIdentifier" namespace="http://www.openarchives.org/OAI/2.0/">
                    <xsl:attribute name="alternateIdentifierType">
                        <xsl:value-of select="'URL'"/>
                    </xsl:attribute>
                    <xsl:value-of
                            select="concat('http://hdl.handle.net/', //*[local-name() = 'resource']/*[local-name()='identifier'])"/>
                </xsl:element>
            </xsl:if>

            <xsl:if test="//*[local-name() = 'resource']/*[local-name()='identifier'][@identifierType='URN']">
                <xsl:element name="alternateIdentifier" namespace="http://www.openarchives.org/OAI/2.0/">
                    <xsl:attribute name="alternateIdentifierType">
                        <xsl:value-of select="'URL'"/>
                    </xsl:attribute>
                    <xsl:value-of
                            select="concat('http://nbn-resolving.org/', //*[local-name() = 'resource']/*[local-name()='identifier'])"/>
                </xsl:element>
            </xsl:if>

            <xsl:if test="//*[local-name() = 'resource']/*[local-name()='identifier'][@identifierType='DOI']">
                <xsl:element name="alternateIdentifier" namespace="http://www.openarchives.org/OAI/2.0/">
                    <xsl:attribute name="alternateIdentifierType">
                        <xsl:value-of select="'URL'"/>
                    </xsl:attribute>
                    <xsl:value-of
                            select="concat('http://dx.doi.org/', //*[local-name() = 'resource']/*[local-name()='identifier'])"/>
                </xsl:element>
            </xsl:if>

        </xsl:element>

    </xsl:template>

    <xsl:template match="//*[local-name() = 'resource']/*[local-name()='identifier']">
        <xsl:copy-of select="."/>
        <xsl:if test="not(//*[local-name() = 'resource']/*[local-name()='alternateIdentifiers'])">
            <xsl:element name="alternateIdentifiers" namespace="http://www.openarchives.org/OAI/2.0/">
                <xsl:if test=".[@identifierType='Handle']">
                    <xsl:element name="alternateIdentifier" namespace="http://www.openarchives.org/OAI/2.0/">
                        <xsl:attribute name="alternateIdentifierType">
                            <xsl:value-of select="'URL'"/>
                        </xsl:attribute>
                        <xsl:value-of
                                select="concat('http://hdl.handle.net/', .)"/>
                    </xsl:element>
                </xsl:if>
                <xsl:if test=".[@identifierType='URN']">
                    <xsl:element name="alternateIdentifier" namespace="http://www.openarchives.org/OAI/2.0/">
                        <xsl:attribute name="alternateIdentifierType">
                            <xsl:value-of select="'URL'"/>
                        </xsl:attribute>
                        <xsl:value-of
                                select="concat('http://nbn-resolving.org/', .)"/>
                    </xsl:element>
                </xsl:if>
                <xsl:if test=".[@identifierType='DOI']">
                    <xsl:element name="alternateIdentifier" namespace="http://www.openarchives.org/OAI/2.0/">
                        <xsl:attribute name="alternateIdentifierType">
                            <xsl:value-of select="'URL'"/>
                        </xsl:attribute>
                        <xsl:value-of
                                select="concat('http://dx.doi.org/', .)"/>
                    </xsl:element>
                </xsl:if>

            </xsl:element>
        </xsl:if>

    </xsl:template>


    <xsl:template match="//*[local-name() = 'header']">
        <xsl:copy>
            <xsl:apply-templates select="node()|@*"/>
            <xsl:element name="dr:dateOfTransformation">
                <xsl:value-of select="$transDate"/>
            </xsl:element>
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>
<!-- 20210224 , title: xslt_cleaning_datarepo_datacite , copy from production -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:oaf="http://namespace.openaire.eu/oaf" xmlns:oai="http://www.openarchives.org/OAI/2.0/" xmlns:datacite="http://datacite.org/schema/kernel-3" xmlns:TransformationFunction="eu.dnetlib.data.collective.transformation.core.xsl.ext.TransformationFunctionProxy" xmlns:dr="http://www.driver-repository.eu/namespace/dr" exclude-result-prefixes="TransformationFunction" extension-element-prefixes="TransformationFunction" version="2.0">
    <xsl:param name="varOfficialName" />
    <xsl:param name="varDsType" />
    <xsl:param name="varDataSourceId" />
    <xsl:param name="varFP7" select="'corda_______::'" />
    <xsl:param name="varH2020" select="'corda__h2020::'" />
    <xsl:param name="varAKA" select="'aka_________::'" />
    <xsl:param name="varARC" select="'arc_________::'" />
    <xsl:param name="varCONICYT" select="'conicytf____::'" />
    <xsl:param name="varDFG" select="'dfgf________::'" />
    <xsl:param name="varFCT" select="'fct_________::'" />
    <xsl:param name="varFWF" select="'fwf_________::'" />
    <xsl:param name="varHRZZ" select="'irb_hr______::'" /> <!-- HRZZ not found -->
    <xsl:param name="varMESTD" select="'mestd_______::'" />
    <xsl:param name="varMZOS" select="'irb_hr______::'" />
    <xsl:param name="varNHMRC" select="'nhmrc_______::'" />
    <xsl:pasram name="varNIH" select="'nih_________::'" />
    <xsl:param name="varNSF" select="'nsf_________::'" />
    <xsl:param name="varNWO" select="'nwo_________::'" />
    <xsl:param name="varRCUK" select="'rcuk________::'" />
    <xsl:param name="varSFI" select="'sfi_________::'" />
    <xsl:param name="varSGOV" select="'sgov________::'" /> <!-- SGOV to be added, awaiting DOI from Pilar, found project ids not in CSV list? -->
    <xsl:param name="varSNSF" select="'snsf________::'" />
    <xsl:param name="varTARA" select="'taraexp_____::'" /> <!-- TARA to be added, awaiting DOI from André -->
    <xsl:param name="varTUBITAK" select="'tubitakf____::'" />
    <xsl:param name="varWT" select="'wt__________::'" />
    <xsl:param name="index" select="0" />
    <xsl:param name="transDate" select="current-dateTime()" />
    <xsl:variable name="tf" select="TransformationFunction:getInstance()" />
    <xsl:variable name="datasourcePrefix" select="normalize-space(//oaf:datasourceprefix)" />
    <xsl:template match="/">
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
                <xsl:apply-templates select="//*[local-name() = 'metadata']//*[local-name() = 'resource']" /> <!-- for CoCoON many deleted records appeared among the transformed records -->
                <xsl:if test="//oai:header/@status='deleted'">
                    <xsl:call-template name="terminate" />
                </xsl:if>
                <xsl:for-each select="//*[local-name() = 'resource']/*[local-name()='identifier'][@identifierType='Handle'][not(. = '123456789') and not(starts-with(., 'textgrid:'))]">
                    <oaf:identifier>
                        <xsl:attribute name="identifierType">
                            <xsl:value-of select="'handle'" />
                        </xsl:attribute>
                        <xsl:if test="contains(., '://hdl.handle.net/')">
                            <xsl:value-of select="substring-after(., '://hdl.handle.net/')" />
                        </xsl:if>
                        <xsl:if test="not(contains(., '://hdl.handle.net/'))">
                            <xsl:value-of select="." />
                        </xsl:if>
                    </oaf:identifier>
                </xsl:for-each>
                <xsl:for-each select="//*[local-name() = 'resource']/*[local-name()='identifier'][@identifierType='DOI']">
                    <oaf:identifier>
                        <xsl:attribute name="identifierType">
                            <xsl:value-of select="'doi'" />
                        </xsl:attribute> <!--
					<xsl:value-of select="."/>
					-->
                        <xsl:if test="contains(., '://dx.doi.org/')">
                            <xsl:value-of select="substring-after(., '://dx.doi.org/')" />
                        </xsl:if>
                        <xsl:if test="not(contains(., '://dx.doi.org/'))">
                            <xsl:value-of select="." />
                        </xsl:if>
                    </oaf:identifier>
                </xsl:for-each>
                <xsl:if test="//*[local-name()='date']/@dateType='Available' and //*[local-name()='datasourceprefix']!='r33ffb097cef'">
                    <xsl:variable name="varEmbargoEndDate" select="TransformationFunction:convertString($tf, normalize-space(//*[local-name()='date'][@dateType='Available']), 'DateISO8601')" />
                    <xsl:choose>
                        <xsl:when test="string-length($varEmbargoEndDate) &gt; 0">
                            <oaf:embargoenddate>
                                <xsl:value-of select="$varEmbargoEndDate" />
                            </oaf:embargoenddate>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:call-template name="terminate" />
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:if> <!--
					<xsl:choose>

					or //*[local-name()='resourceType']/@resourceTypeGeneral/lower-case()='software' or //*[local-name()='resourceType']/@resourceTypeGeneral/lower-case()='software' or //*[local-name()='resourceType']/@resourceTypeGeneral/lower-case()='Film' or //*[local-name()='resourceType']/@resourceTypeGeneral/lower-case()='Sound' or //*[local-name()='resourceType']/@resourceTypeGeneral/lower-case()='PhysicalObject'  or //*[local-name()='resourceType']/@resourceTypeGeneral/lower-case()='Audiovisual'">
					<xsl:when test="lower-case(//*[local-name()='resourceType']/@resourceTypeGeneral)=('dataset', 'software', 'collection', 'film', 'sound', 'physicalobject', 'audiovisual')">

					<xsl:when test="lower-case(//*[local-name()='resourceType']/@resourceTypeGeneral)=('dataset', 'software', 'collection', 'film', 'sound', 'physicalobject', 'audiovisual', 'model', 'workflow', 'service', 'image') or  //*[local-name()='resourceType'][lower-case(@resourceTypeGeneral)='other' and lower-case(.)=('study', 'research data', 'image', 'photos et images')] or //*[local-name()='resourceType'][lower-case(.)='article'] or (//*[local-name()='resourceType'][lower-case(./@resourceTypeGeneral)='other' and lower-case(.)=('study', 'egi virtual appliance')])">

					</xsl:when>
					<xsl:otherwise>
					<xsl:call-template name="terminate"/>
					</xsl:otherwise>
					</xsl:choose>
					-->
                <!--
					<dr:CobjCategory>
					<xsl:value-of
					select="TransformationFunction:convertString($tf, distinct-values(//*[local-name()='resourceType']/@resourceTypeGeneral), 'TextTypologies')" />
					</dr:CobjCategory>

					<dr:CobjCategory>
					<xsl:variable name='varCobjCategory' select="TransformationFunction:convertString($tf, distinct-values(//*[local-name()='resourceType']/@resourceTypeGeneral), 'TextTypologies')" />
					<xsl:attribute name="type" select="TransformationFunction:convertString($tf, $varCobjCategory, 'SuperTypes')"/>
					<xsl:value-of select="$varCobjCategory" />
					</dr:CobjCategory>
					-->
                <xsl:variable name="varTypLst" select="distinct-values((//*[local-name()='resourceType']/(., @resourceTypeGeneral)))" />
                <xsl:variable name="varCobjCatLst" select="distinct-values((for $i in $varTypLst      return TransformationFunction:convertString($tf, normalize-space($i), 'TextTypologies')))" />
                <xsl:variable name="varCobjSupLst" select="for $i in $varCobjCatLst      return concat($i, '###', TransformationFunction:convertString($tf, normalize-space($i), 'SuperTypes'))" />
                <dr:CobjCategory>
                    <xsl:choose>
                        <xsl:when test="count($varCobjSupLst[not(substring-after(., '###') = 'other') and not(substring-before(., '###') = ('0038', '0039', '0040'))]) &gt; 0">
                            <xsl:variable name="varCobjSup" select="$varCobjSupLst[not(substring-after(., '###') = 'other') and not(substring-before(., '###') = ('0038', '0039', '0040'))][1]" />
                            <xsl:attribute name="type" select="substring-after($varCobjSup, '###')" />
                            <xsl:value-of select="substring-before($varCobjSup, '###')" />
                        </xsl:when>
                        <xsl:when test="count($varCobjSupLst[not(substring-after(., '###') = 'other')]) &gt; 0">
                            <xsl:variable name="varCobjSup" select="$varCobjSupLst[not(substring-after(., '###') = 'other')][1]" />
                            <xsl:attribute name="type" select="substring-after($varCobjSup, '###')" />
                            <xsl:value-of select="substring-before($varCobjSup, '###')" />
                        </xsl:when>
                        <xsl:when test="count($varCobjSupLst[not(substring-before(., '###') = ('0020', '0000'))]) &gt; 0">
                            <xsl:variable name="varCobjSup" select="$varCobjSupLst[not(substring-before(., '###') = ('0020', '0000'))][1]" />
                            <xsl:attribute name="type" select="substring-after($varCobjSup, '###')" />
                            <xsl:value-of select="substring-before($varCobjSup, '###')" />
                        </xsl:when>
                        <xsl:when test="count($varCobjSupLst[not(substring-before(., '###') = ('0000'))]) &gt; 0">
                            <xsl:variable name="varCobjSup" select="$varCobjSupLst[not(substring-before(., '###') = ('0000'))][1]" />
                            <xsl:attribute name="type" select="substring-after($varCobjSup, '###')" />
                            <xsl:value-of select="substring-before($varCobjSup, '###')" />
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:attribute name="type" select="'other'" />
                            <xsl:value-of select="'0000'" />
                        </xsl:otherwise>
                    </xsl:choose>
                </dr:CobjCategory> <!-- review status --> <!-- no review hints found in resource type declarations, no version declarations found -->
                <xsl:variable name="varRefereedConvt" select="for $i in (      //*[local-name()='resourceType']/(., @resourceTypeGeneral), //oai:setSpec, //*[local-name()='description'])      return TransformationFunction:convertString($tf, normalize-space($i), 'ReviewLevels')" /> <!--
					//<xsl:variable name="varRefereedIdntf" select="//*[local-name()=('identifier', 'alternateIdentifier')][matches(lower-case(.), '.*[\s\-\.\\_/:]preprints?[\s\-\.\\_/:].*')]/'0002' "/>
					-->
                <xsl:variable name="varRefereedIdntf" select="(      //*[local-name()=('identifier', 'alternateIdentifier')][count(//*[local-name()=('metadata', 'resource')]//*[local-name()=('identifier', 'alternateIdentifier')]) = 1][matches(lower-case(.), '(^|.*[\.\-_\\/\s\(\)%\d#:])pre[\.\-_\\/\s\(\)%\d#:]?prints?([\.\-_\\/\s\(\)%\d#:].*)?$')]/'0002',      //*[local-name()=('identifier', 'alternateIdentifier')][count(//*[local-name()=('metadata', 'resource')]//*[local-name()=('identifier', 'alternateIdentifier')]) = 1][matches(lower-case(.), '(^|.*[\.\-_\\/\s\(\)%\d#:])refereed([\.\-_\\/\s\(\)%\d#:].*)?$')]/'0001',      //*[local-name()=('identifier', 'alternateIdentifier')][count(//*[local-name()=('metadata', 'resource')]//*[local-name()=('identifier', 'alternateIdentifier')]) = 1][matches(lower-case(.), '.*-peer-reviewed-(fulltext-)?article-.*')]/'0001')" />
                <xsl:variable name="varRefereedVersn" select="(//*[local-name()='version'][matches(lower-case(.), '.*peer[\s\-\.\\_/:%]?reviewed.*')]/'0001',      //*[local-name()='version'][matches(normalize-space(lower-case(.)), '^(v|vs|version|rel|release)?[\s\.\-_]*0$')]/'0002',      //*[local-name()='version'][matches(lower-case(.), '(^|[\s\-\.\\_/:%].*)(beta|draft|trial|test)([\s\-\.\\_/:%].*|$)')]/'0002',      //*[local-name()='version'][matches(lower-case(.), '.*submi(tted|ssion|ttal).*')]/'0002') " />
                <xsl:variable name="varRefereedOther" select="(//*[local-name()='publisher'][matches(lower-case(.), '.*[\s\-\.\\_/:%]pre[\s\-\.\\_/:%]?prints?([\s\-\.\\_/:%].*|$)')]/'0002',      //*[local-name()='description'][matches(lower-case(.), '^peer[\s\-\.\\_/:%]?reviewed$')]/'0001',      //*[local-name()='description'][matches(lower-case(.), '^pre[\s\-\.\\_/:%]?prints?$')]/'0002') " />
                <xsl:variable name="varRefereedReltn" select="//*[local-name() = 'relatedIdentifier'][./@relationType/lower-case(.)='isreviewedby']/'0001'" />
                <xsl:variable name="varRefereedDesct" select="(//*[local-name() = 'description']      [matches(lower-case(.), '.*(this\s*book|this\s*volume|it)\s*constitutes\s*the\s*(thoroughly\s*)?refereed') or      matches(lower-case(.), '.*peer[\.\-_/\s\(\)]?review\s*under\s*responsibility\s*of.*') or      matches(lower-case(.), '(this|a)\s*(article|preprint)\s*(has\s*been\s*)?(peer[\-\s]*)?reviewed\s*and\s*recommended\s*by\s*peer[\-\s]*community')]/'0001')" />
                <xsl:variable name="varRefereed" select="($varRefereedConvt, $varRefereedIdntf, $varRefereedReltn, $varRefereedVersn, $varRefereedOther, $varRefereedReltn, $varRefereedDesct)" />
                <xsl:choose>
                    <xsl:when test="count($varRefereed[. = '0001']) &gt; 0">
                        <oaf:refereed>
                            <xsl:value-of select="'0001'" />
                        </oaf:refereed>
                    </xsl:when>
                    <xsl:when test="count($varRefereed[. = '0002']) &gt; 0">
                        <oaf:refereed>
                            <xsl:value-of select="'0002'" />
                        </oaf:refereed>
                    </xsl:when>
                </xsl:choose>
                <oaf:dateAccepted>
                    <xsl:variable name="theDate">
                        <xsl:choose>
                            <xsl:when test="string-length(normalize-space(//*[local-name()='date'][@dateType='Issued'])) &gt; 3">
                                <xsl:value-of select="//*[local-name()='date'][@dateType='Issued']" />
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="//*[local-name()='publicationYear']" />
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:variable>
                    <xsl:value-of select="TransformationFunction:convertString($tf, normalize-space($theDate), 'DateISO8601')" />
                </oaf:dateAccepted>
                <xsl:choose>
                    <!--
					<xsl:if test="//*[local-name() = 'datasourceprefix'][.='r310e4cd113d'] and not(boolean(//*[local-name() = 'rights']/@rightsURI ) )]">
					<oaf:skip>
					<xsl:value-of select="TransformationFunction:skipRecord($tf, $index)"/>
					</oaf:skip>
					</xsl:if>
					-->
                    <!--          <xsl:when test="//*[local-name() = 'rights'][starts-with(normalize-space(.), 'info:eu-repo/semantics')]">
					<oaf:accessrights>
					<xsl:value-of select="TransformationFunction:convertString($tf, //*[local-name() = 'rights'][starts-with(normalize-space(.), 'info:eu-repo/semantics')], 'AccessRights')"/>
					</oaf:accessrights>
					</xsl:when>
					-->
                    <xsl:when test="//*[local-name() = 'rights']/@rightsURI[starts-with(normalize-space(.), 'info:eu-repo/semantics')]">
                        <oaf:accessrights>
                            <xsl:value-of select="TransformationFunction:convertString($tf, //*[local-name() = 'rights']/@rightsURI[starts-with(normalize-space(.), 'info:eu-repo/semantics')], 'AccessRights')" />
                        </oaf:accessrights>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:choose>
                            <xsl:when test="//*[local-name() = 'rights'][starts-with(normalize-space(.), 'http://creativecommons.org') or starts-with(normalize-space(.), 'Creative Commons') or starts-with(normalize-space(.),'CC BY 4.0') or starts-with(normalize-space(.), 'GNU LESSER GENERAL PUBLIC LICENSE')]">
                                <oaf:accessrights>
                                    <xsl:text>OPEN</xsl:text>
                                </oaf:accessrights>
                            </xsl:when>
                            <xsl:when test="//*[local-name() = 'rights']/@rightsURI[starts-with(normalize-space(.), 'http://creativecommons.org') or starts-with(normalize-space(.), 'http://opendatacommons.org')]">
                                <oaf:accessrights>
                                    <xsl:text>OPEN</xsl:text>
                                </oaf:accessrights>
                            </xsl:when>
                            <xsl:when test="//*[local-name() = 'rights'][starts-with(normalize-space(.), 'Open access data at least for academic use')]">
                                <oaf:accessrights>
                                    <xsl:text>RESTRICTED</xsl:text>
                                </oaf:accessrights>
                            </xsl:when>
                            <xsl:otherwise>
                                <oaf:accessrights>
                                    <xsl:text>UNKNOWN</xsl:text>
                                </oaf:accessrights>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:otherwise>
                </xsl:choose>
                <xsl:for-each select="//*[local-name()='rights']/@rightsURI[starts-with(normalize-space(.), 'http') and matches(., '.*(/licenses|/publicdomain|unlicense.org/|/legal-and-data-protection-notices|/download/license|/open-government-licence).*')]">
                    <oaf:license>
                        <xsl:value-of select="." />
                    </oaf:license>
                </xsl:for-each>
                <oaf:language>
                    <xsl:value-of select="TransformationFunction:convert($tf, //*[local-name()='language'], 'Languages')" />
                </oaf:language> <!-- country DE for items from TextGrid -->
                <xsl:if test="$varDataSourceId = 're3data_____::r3d100011365'">
                    <oaf:country>DE</oaf:country>
                </xsl:if> <!--
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
                <xsl:for-each select="//*[local-name()='nameIdentifier']">
                    <xsl:if test="matches(normalize-space(.), '(info:eu-repo/grantagreement/ec/fp7/)(\d\d\d\d\d\d)(.*)', 'i')">
                        <oaf:projectid>
                            <xsl:value-of select="concat($varFP7, replace(normalize-space(.), '(info:eu-repo/grantagreement/ec/fp7/)(\d\d\d\d\d\d)(.*)', '$2', 'i'))" />
                        </oaf:projectid>
                    </xsl:if>
                    <xsl:if test="matches(normalize-space(.), '(info:eu-repo/grantagreement/ec/h2020/)(\d\d\d\d\d\d)(.*)', 'i')">
                        <oaf:projectid>
                            <xsl:value-of select="concat($varH2020, replace(normalize-space(.), '(info:eu-repo/grantagreement/ec/h2020/)(\d\d\d\d\d\d)(.*)', '$2', 'i'))" />
                        </oaf:projectid>
                    </xsl:if>
                </xsl:for-each>
                <oaf:hostedBy>
                    <xsl:attribute name="name">
                        <xsl:value-of select="$varOfficialName" />
                    </xsl:attribute>
                    <xsl:attribute name="id">
                        <xsl:value-of select="$varDataSourceId" />
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
            </metadata>
            <xsl:copy-of select="//*[local-name() = 'about']" />
        </record>
    </xsl:template>
    <xsl:template match="node()|@*">
        <xsl:copy>
            <xsl:apply-templates select="node()|@*" />
        </xsl:copy>
    </xsl:template>
    <xsl:template match="//*[local-name() = 'metadata']//*[local-name() = 'resource']">
        <xsl:copy>
            <xsl:apply-templates select="node()|@*" />
        </xsl:copy>
    </xsl:template>
    <xsl:template match="//*[local-name() = 'resource']/*[local-name()='alternateIdentifiers']">
        <xsl:element name="alternateIdentifiers" namespace="http://datacite.org/schema/kernel-3">
            <xsl:copy-of select="./*" />
            <xsl:if test="//*[local-name() = 'resource']/*[local-name()='identifier'][@identifierType='Handle']">
                <xsl:element name="alternateIdentifier" namespace="http://datacite.org/schema/kernel-3">
                    <xsl:attribute name="alternateIdentifierType">
                        <xsl:value-of select="'URL'" />
                    </xsl:attribute>
                    <xsl:value-of select="concat('http://hdl.handle.net/', //*[local-name() = 'resource']/*[local-name()='identifier'])" />
                </xsl:element>
            </xsl:if>
            <xsl:if test="//*[local-name() = 'resource']/*[local-name()='identifier'][@identifierType='URN']">
                <xsl:element name="alternateIdentifier" namespace="http://datacite.org/schema/kernel-3">
                    <xsl:attribute name="alternateIdentifierType">
                        <xsl:value-of select="'URL'" />
                    </xsl:attribute>
                    <xsl:value-of select="concat('http://nbn-resolving.org/', //*[local-name() = 'resource']/*[local-name()='identifier'])" />
                </xsl:element>
            </xsl:if>
            <xsl:if test="//*[local-name() = 'resource']/*[local-name()='identifier'][@identifierType='DOI']">
                <xsl:element name="alternateIdentifier" namespace="http://datacite.org/schema/kernel-3">
                    <xsl:attribute name="alternateIdentifierType">
                        <xsl:value-of select="'URL'" />
                    </xsl:attribute> <!--
					<xsl:value-of
					select="concat('http://dx.doi.org/', //*[local-name() = 'resource']/*[local-name()='identifier'])" />
					-->
                    <xsl:value-of select="//*[local-name() = 'resource']/(*[local-name()='identifier'][not(contains(., '://dx.doi.org/'))]/concat('http://dx.doi.org/', .), *[local-name()='identifier'][contains(., '://dx.doi.org/')])" />
                </xsl:element>
            </xsl:if>
        </xsl:element>
    </xsl:template>
    <xsl:template match="//*[local-name() = 'resource']/*[local-name()='identifier']">
        <!-- cut off DOI resolver prefix to just get the number part -->
        <xsl:if test=".[@identifierType='DOI'][contains(., '://dx.doi.org/')]">
            <xsl:element name="identifier" namespace="http://datacite.org/schema/kernel-3">
                <xsl:attribute name="identifierType">
                    <xsl:value-of select="'DOI'" />
                </xsl:attribute>
                <xsl:value-of select="substring-after(., '://dx.doi.org/')" />
            </xsl:element>
        </xsl:if>
        <xsl:copy-of select=".[not(contains(., '://dx.doi.org/'))]" /> <!--
					<xsl:copy-of select="."/>
					-->
        <xsl:if test="not(//*[local-name() = 'resource']/*[local-name()='alternateIdentifiers'])">
            <xsl:element name="alternateIdentifiers" namespace="http://datacite.org/schema/kernel-3">
                <xsl:if test=".[@identifierType='Handle']">
                    <xsl:element name="alternateIdentifier" namespace="http://datacite.org/schema/kernel-3">
                        <xsl:attribute name="alternateIdentifierType">
                            <xsl:value-of select="'URL'" />
                        </xsl:attribute>
                        <xsl:value-of select="concat('http://hdl.handle.net/', .)" />
                    </xsl:element>
                </xsl:if>
                <xsl:if test=".[@identifierType='URN']">
                    <xsl:element name="alternateIdentifier" namespace="http://datacite.org/schema/kernel-3">
                        <xsl:attribute name="alternateIdentifierType">
                            <xsl:value-of select="'URL'" />
                        </xsl:attribute>
                        <xsl:value-of select="concat('http://nbn-resolving.org/', .)" />
                    </xsl:element>
                </xsl:if>
                <xsl:if test=".[@identifierType='DOI']">
                    <xsl:element name="alternateIdentifier" namespace="http://datacite.org/schema/kernel-3">
                        <xsl:attribute name="alternateIdentifierType">
                            <xsl:value-of select="'URL'" />
                        </xsl:attribute>
                        <xsl:value-of select="concat('http://dx.doi.org/', .)" />
                    </xsl:element>
                </xsl:if>
            </xsl:element>
        </xsl:if> <!-- funding -->
        <xsl:for-each select="//*[local-name()='fundingReference']">
            <!-- FP7 -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100004963', '10.13039/100011199') and matches(./*[local-name()='awardNumber'], '.*(^|[^\d])\d\d\d\d\d\d($|[^\d]).*')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varFP7, replace(./*[local-name()='awardNumber'], '.*(^|[^\d])(\d\d\d\d\d\d)($|[^\d]).*', '$2'))" />
                </oaf:projectid>
            </xsl:if> <!-- H2020 -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/100010661') and matches(./*[local-name()='awardNumber'], '.*(^|[^\d])\d\d\d\d\d\d($|[^\d]).*')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varH2020, replace(./*[local-name()='awardNumber'], '.*(^|[^\d])(\d\d\d\d\d\d)($|[^\d]).*', '$2'))" />
                </oaf:projectid>
            </xsl:if> <!-- AKA -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100002341') or contains(./*[local-name()='funderName'], 'Suomen Akatemia') or contains(./*[local-name()='funderName'], 'Academy of Finland')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varAKA, ./*[local-name()='awardNumber'])" />
                </oaf:projectid>
            </xsl:if> <!-- ARC -->
            <xsl:if test="(substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100000923') or contains(./*[local-name()='funderName'], 'Australian Research Council')) and matches(./*[local-name()='awardNumber'], '^\d{6}$')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varAKA, replace(./*[local-name()='awardNumber'], '.*(^\d{6}$).*', '$2'))" />
                </oaf:projectid>
            </xsl:if> <!-- CONICYT -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100002848') or contains(./*[local-name()='funderName'], 'Comisión Nacional de Investigación Científica y Tecnológica') or contains(./*[local-name()='funderName'], 'CONICYT')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varCONICYT, ./*[local-name()='awardNumber'])" />
                </oaf:projectid>
            </xsl:if> <!-- DFG -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100001659') or contains(./*[local-name()='funderName'], 'Deutsche Forschungsgemeinschaft') or contains(./*[local-name()='funderName'], 'DFG')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varDFG, ./*[local-name()='awardNumber'])" />
                </oaf:projectid>
            </xsl:if> <!-- FCT -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100001871') or contains(./*[local-name()='funderName'], 'Fundação para a Ciência e a Tecnologia')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varFCT, ./*[local-name()='awardNumber'])" />
                </oaf:projectid>
            </xsl:if> <!-- FWF -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100002428') or contains(./*[local-name()='funderName'], 'Fonds zur Förderung der Wissenschaftlichen Forschung') or contains(./*[local-name()='funderName'], 'Austrian Science Fund')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varFWF, ./*[local-name() = 'awardNumber'])" />
                </oaf:projectid>
            </xsl:if> <!-- MESTD -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100001871') or (contains(./*[local-name()='funderName'], 'Ministarstvo Prosvete, Nauke i Tehnolo') and contains(./*[local-name()='funderName'], 'kog Razvoja')) or contains(./*[local-name()='funderName'], 'MESTD')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varMESTD, ./*[local-name()='awardNumber'])" />
                </oaf:projectid>
            </xsl:if> <!-- MZOS -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100006588') or contains(./*[local-name()='funderName'], 'Ministarstvo Znanosti, Obrazovanja i Sporta') or contains(./*[local-name()='funderName'], 'Ministry of Science, Education and Sports')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varMZOS, ./*[local-name()='awardNumber'])" />
                </oaf:projectid>
            </xsl:if> <!-- NHMRC -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100000925') or contains(./*[local-name()='funderName'], 'National Health and Medical Research Council') or contains(./*[local-name()='funderName'], 'NHMRC')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varNHMRC, ./*[local-name()='awardNumber'])" />
                </oaf:projectid>
            </xsl:if> <!-- NIH -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/100000002') or contains(./*[local-name()='funderName'], 'National Institutes of Health')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varNIH, ./*[local-name()='awardNumber'])" />
                </oaf:projectid>
            </xsl:if> <!-- NSF -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org') = ('10.13039/100000001') or contains(./*[local-name()='funderName'], 'National Science Foundation')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varNSF, ./*[local-name()='awardNumber'])" />
                </oaf:projectid>
            </xsl:if> <!-- NWO -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100003246') or contains(./*[local-name()='funderName'], 'Netherlands Organisation for Scientific Research') or contains(./*[local-name()='funderName'], 'Nederlandse Organisatie voor Wetenschappelijk Onderzoek')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varNWO, ./*[local-name()='awardNumber'])" />
                </oaf:projectid>
            </xsl:if> <!-- RCUK -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100000690') or contains(./*[local-name()='funderName'], 'Research Councils UK') or contains(./*[local-name()='funderName'], 'RCUK')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varRCUK, ./*[local-name()='awardNumber'])" />
                </oaf:projectid>
            </xsl:if> <!-- SFI -->
            <xsl:if test="(substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100001602') or contains(./*[local-name()='funderName'], 'Science Foundation Ireland')) and matches(./*[local-name()='awardNumber'], '.*([\dA-Za-z\.\-]+/)+[\dA-Za-z\.\-]+.*')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varSFI, replace(./*[local-name()='awardNumber'], '.*(^|\s)(([\dA-Za-z\.\-]+/)+[\dA-Za-z\.\-]+)($|\s).*', '$2'))" />
                </oaf:projectid>
            </xsl:if> <!-- SNSF -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100001711') or contains(./*[local-name()='funderName'], 'Swiss National Science Foundation') or contains(./*[local-name()='funderName'], 'Schweizerischer Nationalfonds zur Förderung der Wissenschaftlichen Forschung')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varSNSF, ./*[local-name()='awardNumber'])" />
                </oaf:projectid>
            </xsl:if> <!-- TUBITAK -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/501100004410') or contains(./*[local-name()='funderName'], 'Turkish National Science and Research Council') or (contains(./*[local-name()='funderName'], 'Türkiye Bilimsel ve Teknolojik Ara') and contains(./*[local-name()='funderName'], 'rma Kurumu'))">
                <oaf:projectid>
                    <xsl:value-of select="concat($varTUBITAK, ./*[local-name()='awardNumber'])" />
                </oaf:projectid>
            </xsl:if> <!-- WT -->
            <xsl:if test="substring-after(normalize-space(./*[local-name()='funderIdentifier']), 'doi.org/') = ('10.13039/100004440') or contains(./*[local-name()='funderName'], 'Wellcome Trust')">
                <oaf:projectid>
                    <xsl:value-of select="concat($varWT, ./*[local-name()='awardNumber'])" />
                </oaf:projectid>
            </xsl:if>
        </xsl:for-each>
    </xsl:template>
    <xsl:template match="//*[local-name() = 'resource']/*[local-name() = 'relatedIdentifier' and @relatedIdentifierType = 'Handle']">
        <datacite:relatedIdentifier relatedIdentifierType="OPENAIRE" relationType="{./@relationType}">
            <xsl:value-of select="concat($datasourcePrefix, '::', ./text())" />
        </datacite:relatedIdentifier>
    </xsl:template>
    <xsl:template match="//*[local-name() = 'header']">
        <xsl:copy>
            <xsl:apply-templates select="node()|@*" />
            <xsl:element name="dr:dateOfTransformation">
                <xsl:value-of select="$transDate" />
            </xsl:element>
        </xsl:copy>
    </xsl:template>
</xsl:stylesheet>

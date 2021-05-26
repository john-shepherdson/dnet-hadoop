<!-- adapted from PROD at 2021-05-26 -->
<xsl:stylesheet 
        xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
        xmlns:oaf="http://namespace.openaire.eu/oaf" 
        xmlns:oai="http://www.openarchives.org/OAI/2.0/" 
        xmlns:dr="http://www.driver-repository.eu/namespace/dr" 
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
        xmlns:vocabulary="http://eu/dnetlib/transform/clean"
        xmlns:dateCleaner="http://eu/dnetlib/transform/dateISO"
        exclude-result-prefixes="xsl vocabulary dateCleaner" 
        version="2.0">
    <xsl:param name="varOfficialName" />
    <xsl:param name="varDsType" />
    <xsl:param name="varDataSourceId" />
    <xsl:param name="index" select="0" />
    <xsl:param name="transDate" select="current-dateTime()" />

    <xsl:template match="/">
        <xsl:variable name="datasourcePrefix" select="normalize-space(//oaf:datasourceprefix)" />
        <xsl:call-template name="validRecord" />
    </xsl:template>

    <!-- skip/terminate record transformation -->
    <xsl:template name="terminate">
        <xsl:message terminate="yes">
			record is not compliant, transformation is interrupted.
		</xsl:message>
    </xsl:template>

    <!-- validate record -->
    <xsl:template name="validRecord">
        <record>
            <xsl:apply-templates select="//*[local-name() = 'header']" />
            <metadata>
                <xsl:apply-templates select="//*[local-name() = 'metadata']//*[local-name() = 'resource']" /> <!-- for CoCoON many deleted records appeared among the transformed records -->
                <xsl:if test="//oai:header/@status='deleted'">
                    <xsl:call-template name="terminate" />
                </xsl:if>
                <xsl:for-each select="//*[local-name() = 'resource']/*[local-name()='identifier'][@identifierType='Handle'][not(. = '123456789')]">
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
                        </xsl:attribute>
                        <xsl:if test="contains(., '://dx.doi.org/')">
                            <xsl:value-of select="substring-after(., '://dx.doi.org/')" />
                        </xsl:if>
                        <xsl:if test="not(contains(., '://dx.doi.org/'))">
                            <xsl:value-of select="." />
                        </xsl:if>
                    </oaf:identifier>
                </xsl:for-each> <!-- This is the only difference with the generic datacite repo rule: since all datasets from Nakala are Images -->
                <xsl:if test="lower-case(//*[local-name()='resourceType']/@*[local-name()='resourceTypeGeneral']) = 'image'">
                    <oaf:concept>
                        <xsl:attribute name="id">
                            <xsl:value-of select="'dariah'" />
                        </xsl:attribute>
                    </oaf:concept>
                </xsl:if>
                <xsl:if test="//*[local-name()='date']/@dateType='Available' and //*[local-name()='datasourceprefix']!='r33ffb097cef'">
                    <xsl:variable name="varEmbargoEndDate" select="dateCleaner:dateISO( normalize-space(//*[local-name()='date'][@dateType='Available']))" />

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
                </xsl:if>
                <xsl:variable name="varTypLst" select="distinct-values((//*[local-name()='resourceType']/(., @resourceTypeGeneral)))" />
                <xsl:variable name="varCobjCatLst" select="distinct-values((for $i in $varTypLst      return vocabulary:clean( normalize-space($i), 'dnet:publication_resource')))" />
                <xsl:variable name="varCobjSupLst" select="for $i in $varCobjCatLst      return concat($i, '###', vocabulary:clean( normalize-space($i), 'dnet:result_typologies'))" />
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

                <xsl:variable name="varRefereedConvt" select="for $i in (      //*[local-name()='resourceType']/(., @resourceTypeGeneral), //oai:setSpec, //*[local-name()='description'])      return vocabulary:clean( normalize-space($i), 'dnet:review_levels')" />
                <xsl:variable name="varRefereedIdntf" select="(      //*[local-name()=('identifier', 'alternateIdentifier')][count(//*[local-name()=('metadata', 'resource')]//*[local-name()=('identifier', 'alternateIdentifier')]) = 1][matches(lower-case(.), '(^|.*[\.\-_\\/\s\(\)%\d#:])pre[\.\-_\\/\s\(\)%\d#:]?prints?([\.\-_\\/\s\(\)%\d#:].*)?$')]/'0002',      //*[local-name()=('identifier', 'alternateIdentifier')][count(//*[local-name()=('metadata', 'resource')]//*[local-name()=('identifier', 'alternateIdentifier')]) = 1][matches(lower-case(.), '(^|.*[\.\-_\\/\s\(\)%\d#:])refereed([\.\-_\\/\s\(\)%\d#:].*)?$')]/'0001',      //*[local-name()=('identifier', 'alternateIdentifier')][count(//*[local-name()=('metadata', 'resource')]//*[local-name()=('identifier', 'alternateIdentifier')]) = 1][matches(lower-case(.), '.*-peer-reviewed-(fulltext-)?article-.*')]/'0001')" />
                <xsl:variable name="varRefereedVersn" select="(//*[local-name()='version'][matches(lower-case(.), '.*peer[\s\-\.\\_/:%]?reviewed.*')]/'0001',      //*[local-name()='version'][matches(normalize-space(lower-case(.)), '^(v|vs|version|rel|release)?[\s\.\-_]*0$')]/'0002',      //*[local-name()='version'][matches(lower-case(.), '(^|[\s\-\.\\_/:%].*)(beta|draft|trial|test)([\s\-\.\\_/:%].*|$)')]/'0002',      //*[local-name()='version'][matches(lower-case(.), '.*submi(tted|ssion|ttal).*')]/'0002') " />
                <xsl:variable name="varRefereedOther" select="(//*[local-name()='publisher'][matches(lower-case(.), '.*[\s\-\.\\_/:%]pre[\s\-\.\\_/:%]?prints?([\s\-\.\\_/:%].*|$)')]/'0002',      //*[local-name()='description'][matches(lower-case(.), '^peer[\s\-\.\\_/:%]?reviewed$')]/'0001',      //*[local-name()='description'][matches(lower-case(.), '^pre[\s\-\.\\_/:%]?prints?$')]/'0002') " />
                <xsl:variable name="varRefereedReltn" select="//*[local-name() = 'relatedIdentifier'][./@relationType/lower-case(.)='isreviewedby']/'0001'" />
                <xsl:variable name="varRefereedDesct" select="(//*[local-name() = 'description']      [matches(lower-case(.), '.*(this\s*book|this\s*volume|it)\s*constitutes\s*the\s*(thoroughly\s*)?refereed') or      matches(lower-case(.), '.*peer[\.\-_/\s\(\)]?review\s*under\s*responsibility\s*of.*') or      matches(lower-case(.), '(this|a)\s*(article|preprint)\s*(has\s*been\s*)?(peer[\-\s]*)?reviewed\s*and\s*recommended\s*by\s*peer[\-\s]*community')]/'0001')" />
                <xsl:variable name="varRefereed" select="($varRefereedConvt, $varRefereedIdntf, $varRefereedReltn, $varRefereedVersn, $varRefereedOther, $varRefereedReltn, $varRefereedDesct)" />

<!--                
                <xsl:variable name="varRefereedConvt" select="for $i in (      //*[local-name()='resourceType']/(., @resourceTypeGeneral), //oai:setSpec, //*[local-name()='description'])      return TransformationFunction:convertString($tf, normalize-space($i), 'ReviewLevels')" />
                <xsl:variable name="varRefereedIdntf" select="(      //*[local-name()=('identifier', 'alternateIdentifier')][count(//*[local-name()=('metadata', 'resource')]//*[local-name()=('identifier', 'alternateIdentifier')]) = 1][matches(lower-case(.), '(^|.*[\.\-_\\/\s\(\)%\d#:])pre[\.\-_\\/\s\(\)%\d#:]?prints?([\.\-_\\/\s\(\)%\d#:].*)?$')]/'0002',      //*[local-name()=('identifier', 'alternateIdentifier')][count(//*[local-name()=('metadata', 'resource')]//*[local-name()=('identifier', 'alternateIdentifier')]) = 1][matches(lower-case(.), '(^|.*[\.\-_\\/\s\(\)%\d#:])refereed([\.\-_\\/\s\(\)%\d#:].*)?$')]/'0001',      //*[local-name()=('identifier', 'alternateIdentifier')][count(//*[local-name()=('metadata', 'resource')]//*[local-name()=('identifier', 'alternateIdentifier')]) = 1][matches(lower-case(.), '.*-peer-reviewed-(fulltext-)?article-.*')]/'0001')" />
                <xsl:variable name="varRefereedVersn" select="(//*[local-name()='version'][matches(lower-case(.), '.*peer[\s\-\.\\_/:%]?reviewed.*')]/'0001',      //*[local-name()='version'][matches(normalize-space(lower-case(.)), '^(v|vs|version|rel|release)?[\s\.\-_]*0$')]/'0002',      //*[local-name()='version'][matches(lower-case(.), '(^|[\s\-\.\\_/:%].*)(beta|draft|trial|test)([\s\-\.\\_/:%].*|$)')]/'0002',      //*[local-name()='version'][matches(lower-case(.), '.*submi(tted|ssion|ttal).*')]/'0002') " />
                <xsl:variable name="varRefereedOther" select="(//*[local-name()='publisher'][matches(lower-case(.), '.*[\s\-\.\\_/:%]pre[\s\-\.\\_/:%]?prints?([\s\-\.\\_/:%].*|$)')]/'0002',      //*[local-name()='description'][matches(lower-case(.), '^peer[\s\-\.\\_/:%]?reviewed$')]/'0001',      //*[local-name()='description'][matches(lower-case(.), '^pre[\s\-\.\\_/:%]?prints?$')]/'0002') " />
                <xsl:variable name="varRefereedReltn" select="//*[local-name() = 'relatedIdentifier'][./@relationType/lower-case(.)='isreviewedby']/'0001'" />
                <xsl:variable name="varRefereedDesct" select="(//*[local-name() = 'description']      [matches(lower-case(.), '.*(this\s*book|this\s*volume|it)\s*constitutes\s*the\s*(thoroughly\s*)?refereed') or      matches(lower-case(.), '.*peer[\.\-_/\s\(\)]?review\s*under\s*responsibility\s*of.*') or      matches(lower-case(.), '(this|a)\s*(article|preprint)\s*(has\s*been\s*)?(peer[\-\s]*)?reviewed\s*and\s*recommended\s*by\s*peer[\-\s]*community')]/'0001')" />
                <xsl:variable name="varRefereed" select="($varRefereedConvt, $varRefereedIdntf, $varRefereedReltn, $varRefereedVersn, $varRefereedOther, $varRefereedReltn, $varRefereedDesct)" />
-->
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
                   <xsl:value-of select="dateCleaner:dateISO( normalize-space($theDate) )" />
               </oaf:dateAccepted>
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
                   <xsl:value-of select="dateCleaner:dateISO( normalize-space($theDate) )" />
               </oaf:dateAccepted>
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
                   <xsl:value-of select="dateCleaner:dateISO( normalize-space($theDate) )" />
               </oaf:dateAccepted>
                <xsl:choose>
                    <xsl:when test="//*[local-name() = 'rights']/@rightsURI[starts-with(normalize-space(.), 'info:eu-repo/semantics')]">
                        <oaf:accessrights>
                            <xsl:value-of select="vocabulary:clean( //*[local-name() = 'rights']/@rightsURI[starts-with(normalize-space(.), 'info:eu-repo/semantics')], 'dnet:access_modes')" />
                        </oaf:accessrights>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:choose>
                            <xsl:when test="//*[local-name() = 'rights'][starts-with(normalize-space(.), 'http://creativecommons.org') or starts-with(normalize-space(.), 'Creative Commons') or starts-with(normalize-space(.), 'GNU LESSER GENERAL PUBLIC LICENSE')]">
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
                    <xsl:value-of select="vocabulary:clean( //*[local-name()='language'], 'dnet:languages')" />
                </oaf:language>
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
    <xsl:template match="//*[local-name() = 'resource']/*[local-name()='identifier']">
        <xsl:if test=".[@identifierType='Handle'][contains(., '://hdl.handle.net/')]">
            <xsl:element name="identifier" namespace="http://datacite.org/schema/kernel-4">
                <xsl:attribute name="identifierType">
                    <xsl:value-of select="'Handle'" />
                </xsl:attribute>
                <xsl:value-of select="." />
            </xsl:element>
        </xsl:if>
    </xsl:template>
    <xsl:template match="//*[local-name() = 'resource']/*[local-name()='alternateIdentifier']">
        <xsl:choose>
            <xsl:when test="@alternateIdentifierType">
                <xsl:copy-of select="." />
            </xsl:when>
            <xsl:otherwise>
                <xsl:element name="alternateIdentifier" namespace="http://datacite.org/schema/kernel-4">
                    <xsl:attribute name="alternateIdentifierType">
                        <xsl:value-of select="./@identifierType" />
                    </xsl:attribute>
                    <xsl:value-of select="." />
                </xsl:element>
            </xsl:otherwise>
        </xsl:choose>
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

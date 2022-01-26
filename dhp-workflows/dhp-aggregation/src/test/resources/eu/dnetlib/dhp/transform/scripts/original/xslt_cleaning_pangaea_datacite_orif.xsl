<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.1"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:oaf="http://namespace.openaire.eu/oaf"
    xmlns:dr="http://www.driver-repository.eu/namespace/dr"
    xmlns:TransformationFunction="eu.dnetlib.data.collective.transformation.core.xsl.ext.TransformationFunctionProxy"
    extension-element-prefixes="TransformationFunction"
    exclude-result-prefixes="TransformationFunction">
    <xsl:param name="varOfficialName"/>
    <xsl:param name="varDsType"/>
    <xsl:param name="varDataSourceId"/>
    <xsl:param name="varCorda" select="'corda_______::'"/>
    <xsl:param name="varCordaH2020" select="'corda__h2020::'"/>
    <xsl:param name="index" select="0"/>
    <xsl:param name="transDate" select="current-dateTime()"/>
    <xsl:variable name="tf" select="TransformationFunction:getInstance()"/>
    <xsl:template match="/">
        <xsl:variable name="datasourcePrefix" select="normalize-space(//oaf:datasourceprefix)"/>
        <xsl:call-template name="validRecord"/>
    </xsl:template>
    <xsl:template name="validRecord">
        <record>
            <xsl:apply-templates select="//*[local-name() = 'header']"/>
            <metadata>
                <xsl:copy-of select="//*[local-name() = 'metadata']/*[local-name() = 'resource']"/>
                <xsl:copy-of
                    select="//*[local-name() = 'metadata']/*[local-name() = 'relatedPublication']"
                    copy-namespaces="no"/>
                <xsl:copy-of
                    select="//*[local-name() = 'metadata']/*[local-name() = 'relatedDataSet']"
                    copy-namespaces="no"/>
                <xsl:if test="//*[local-name() = 'date']/@dateType = 'Available'">
                    <xsl:variable name="varEmbargoEndDate"
                        select="TransformationFunction:convertString($tf, normalize-space(//*[local-name() = 'date'][@dateType = 'Available']), 'DateISO8601')"/>
                    <xsl:choose>
                        <xsl:when test="string-length($varEmbargoEndDate) > 0">
                            <oaf:embargoenddate>
                                <xsl:value-of select="$varEmbargoEndDate"/>
                            </oaf:embargoenddate>
                        </xsl:when>
                        <xsl:otherwise>
                            <oaf:skip>
                                <xsl:value-of
                                    select="TransformationFunction:skipRecord($tf, $index)"/>
                            </oaf:skip>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:if>
                <dr:CobjCategory>
                    <xsl:value-of
                        select="TransformationFunction:convertString($tf, //*[local-name() = 'resourceType']/@resourceTypeGeneral, 'TextTypologies')"
                    />
                </dr:CobjCategory>
                <oaf:dateAccepted>
                    <xsl:value-of
                        select="TransformationFunction:convertString($tf, normalize-space(//*[local-name() = 'publicationYear']), 'DateISO8601')"
                    />
                </oaf:dateAccepted>
                <xsl:choose>
                    <xsl:when
                        test="//*[local-name() = 'rights'][starts-with(normalize-space(.), 'info:eu-repo/semantics')]">
                        <oaf:accessrights>
                            <xsl:value-of
                                select="TransformationFunction:convertString($tf, //*[local-name() = 'rights'][starts-with(normalize-space(.), 'info:eu-repo/semantics')], 'AccessRights')"
                            />
                        </oaf:accessrights>
                    </xsl:when>
                    <xsl:when
                        test="//*[local-name() = 'rights']/@rightsURI[starts-with(normalize-space(.), 'info:eu-repo/semantics')]">
                        <oaf:accessrights>
                            <xsl:value-of
                                select="TransformationFunction:convertString($tf, //*[local-name() = 'rights']/@rightsURI[starts-with(normalize-space(.), 'info:eu-repo/semantics')], 'AccessRights')"
                            />
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
                        select="TransformationFunction:convert($tf, //*[local-name() = 'language'], 'Languages')"
                    />
                </oaf:language>
                <!--
<xsl:if test="//*[local-name() = 'rights'][starts-with(normalize-space(.), 'info:eu-repo/semantics/embargoedAccess')]">
<oaf:embargoenddate>
<xsl:value-of select="//*[local-name()='date']/@dateType='Available'"/>
</oaf:embargoenddate>
</xsl:if>
-->
<!-- I am not sure the following foreach works and it is useful...nameIdentifie seems not to be an XML element in pangaea format -->

 <xsl:for-each select="//*[local-name() = 'nameIdentifier']">

                <xsl:if test="string-length(substring-after(normalize-space(.), 'info:eu-repo/grantAgreement/EC/FP7/')) = 6">
                        <oaf:projectid>
                            <xsl:value-of
                                select="TransformationFunction:regExpr($tf, normalize-space(.), $varCorda, 's/^(.*info:eu-repo\/grantAgreement\/EC\/FP7\/)//gm')"
                            />
                        </oaf:projectid>
                    </xsl:if>
                    <xsl:if test="starts-with(normalize-space(.), 'tar')">
                        <oaf:projectid>
                            <xsl:value-of select="normalize-space(.)"/>
                        </oaf:projectid>
                    </xsl:if>
                    <xsl:if test="string-length(substring-after(normalize-space(.), 'info:eu-repo/grantAgreement/EC/H2020/')) >= 6">
							<oaf:projectid>
                            <xsl:value-of
                                select="TransformationFunction:regExpr($tf, normalize-space(.), $varCordaH2020, 's/^(.*info:eu-repo\/grantAgreement\/EC\/H2020\/)//gm')" />
                        </oaf:projectid>
						</xsl:if>
                </xsl:for-each>

                <xsl:for-each select="//*[local-name() = 'projectid']">
                     <xsl:choose>
						<xsl:when test="string-length(substring-after(normalize-space(.), 'info:eu-repo/grantAgreement/EC/FP7/')) > 0">
							<oaf:projectid>
                            <xsl:value-of
                                select="TransformationFunction:regExpr($tf, normalize-space(.), $varCorda, 's/^(.*info:eu-repo\/grantAgreement\/EC\/FP7\/)//gm')" />
                        </oaf:projectid>
						</xsl:when>
						<xsl:when test="string-length(substring-after(normalize-space(.), 'info:eu-repo/grantAgreement/EC/H2020/')) > 0">
							<oaf:projectid>
                            <xsl:value-of
                                select="TransformationFunction:regExpr($tf, normalize-space(.), $varCordaH2020, 's/^(.*info:eu-repo\/grantAgreement\/EC\/H2020\/)//gm')" />
                        </oaf:projectid>
						</xsl:when>
						<xsl:otherwise>
							<oaf:projectid><xsl:value-of select="normalize-space(.)"/></oaf:projectid>
						</xsl:otherwise>
					</xsl:choose>
                </xsl:for-each>



<!--
                <xsl:for-each select="//*[local-name() = 'projectid']">

                <xsl:if test="string-length(substring-after(normalize-space(.), 'info:eu-repo/grantAgreement/EC/FP7/')) >0">
                        <oaf:projectid>
                            <xsl:value-of
                                select="TransformationFunction:regExpr($tf, normalize-space(.), $varCorda, 's/^(.*info:eu-repo\/grantAgreement\/EC\/FP7\/)//gm')"
                            />
                        </oaf:projectid>
                    </xsl:if>
                    <xsl:if test="starts-with(normalize-space(.), 'tar')">
                        <oaf:projectid>
                            <xsl:value-of select="normalize-space(.)"/>
                        </oaf:projectid>
                    </xsl:if>
                </xsl:for-each>

                <xsl:for-each select="//*[local-name() = 'nameIdentifier']">
                     <xsl:choose>
						<xsl:when test="string-length(substring-after(normalize-space(.), 'info:eu-repo/grantAgreement/EC/FP7/')) >= 6">
							<oaf:projectid>
                            <xsl:value-of
                                select="TransformationFunction:regExpr($tf, normalize-space(.), $varCorda, 's/^(.*info:eu-repo\/grantAgreement\/EC\/FP7\/)//gm')" />
                        </oaf:projectid>
						</xsl:when>
						<xsl:when test="string-length(substring-after(normalize-space(.), 'info:eu-repo/grantAgreement/EC/H2020/')) >= 6">
							<oaf:projectid>
                            <xsl:value-of
                                select="TransformationFunction:regExpr($tf, normalize-space(.), $varCordaH2020, 's/^(.*info:eu-repo\/grantAgreement\/EC\/H2020\/)//gm')" />
                        </oaf:projectid>
						</xsl:when>
						<xsl:otherwise>
							 <xsl:if test="starts-with(normalize-space(.),'info:eu-repo/grantAgreement')">
                                <oaf:projectid>
                                <xsl:value-of select="normalize-space(.)"/>
                            </oaf:projectid>
                            </xsl:if>
						</xsl:otherwise>
					</xsl:choose>
                </xsl:for-each>

-->
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
    <!--
<xsl:template match="//*[local-name()='language']">
<oaf:language>
<xsl:value-of select="TransformationFunction:convert($tf, //*[local-name()='language'], 'Languages')" />
</oaf:language>
</xsl:template>
-->
    <xsl:template match="node() | @*">
        <xsl:copy>
            <xsl:apply-templates select="node() | @*"/>
        </xsl:copy>
    </xsl:template>
    <xsl:template match="//*[local-name() = 'header']">
        <xsl:copy>
            <xsl:apply-templates select="node() | @*"/>
            <xsl:element name="dr:dateOfTransformation">
                <xsl:value-of select="$transDate"/>
            </xsl:element>
        </xsl:copy>
    </xsl:template>
</xsl:stylesheet>

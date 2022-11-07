<!-- apart of literature v4: xslt_cleaning_oaiOpenaire_datacite_ExchangeLandingpagePid ; transformation script production , 2021-02-18 -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="2.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:oaf="http://namespace.openaire.eu/oaf" xmlns:dr="http://www.driver-repository.eu/namespace/dr" xmlns:datacite="http://datacite.org/schema/kernel-4" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dri="http://www.driver-repository.eu/namespace/dri" xmlns:oaire="http://namespace.openaire.eu/schema/oaire/" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:TransformationFunction="http://eu/dnetlib/transform/functionProxy" exclude-result-prefixes="xsl TransformationFunction">

    <xsl:param name="varOfficialName" />
    <xsl:param name="varDsType" />
    <xsl:param name="varDataSourceId" />

    <xsl:param name="varFP7" select="'corda_______::'" />
    <xsl:param name="varH2020" select="'corda__h2020::'" />

    <xsl:template match="/">
        <xsl:variable name="datasourcePrefix" select="normalize-space(//oaf:datasourceprefix)" />
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
                                <xsl:attribute name="identifierType" select="'DOI'" />
                                <xsl:value-of select="//datacite:identifier[@identifierType='DOI'][contains(., '://dx.doi.org/')]/substring-after(., '://dx.doi.org/'),
                                                                       (//datacite:identifier, //datacite:alternateIdentifier)[@identifierType='DOI' or @alternateIdentifierType='DOI'][not(contains(., '://dx.doi.org/'))],
                                                                       //datacite:identifier[contains(., '://dx.doi.org/10.')]/substring-after(., '://dx.doi.org/')" />
                            </datacite:identifier>
                        </xsl:when>
                        <xsl:when test="//datacite:identifier[lower-case(@identifierType)='handle'], //datacite:identifier[contains(., '://refubium.fu-berlin.de/handle/')]">
                            <datacite:identifier>
                                <xsl:attribute name="identifierType" select="'handle'" />
                                <xsl:value-of select="//datacite:identifier[lower-case(@identifierType)='handle'][contains(., '://hdl.handle.net/')]/substring-after(., '://hdl.handle.net/'),
                                                                       //datacite:identifier[lower-case(@identifierType)='handle'][contains(., 'info:hdl:')]/substring-after(., 'info:hdl:'),
                                                                       //datacite:identifier[lower-case(@identifierType)='handle'][contains(., '/handle/')]/substring-after(., '/handle/'),
                                                                       //datacite:identifier[contains(., '://refubium.fu-berlin.de/handle/')]/substring-after(., '/handle/'),
                                                                       //datacite:identifier[lower-case(@identifierType)='handle'][not(contains(., '://hdl.handle.net/')) and not(contains(., 'info:hdl:')) and not(contains(., '/handle/'))]" />
                            </datacite:identifier>
                        </xsl:when>

                        <xsl:when test="//oaf:datasourceprefix = ('od______4225', 'r3110ae70d66') and not(//datacite:identifier[@identifierType='DOI'] and //datacite:identifier[@identifierType='URL'])">
                            <datacite:identifier>
                                <xsl:attribute name="identifierType" select="'URL'" />
                                <xsl:value-of select="//datacite:identifier[@identifierType='URL']" />
                            </datacite:identifier>
                        </xsl:when>

                        <xsl:when test="//dri:recordIdentifier[contains(., 'phaidra.univie.ac.at')] and //datacite:alternateIdentifier[@alternateIdentifierType='Handle' and starts-with(., '11353/10.')]">
                            <datacite:identifier>
                                <xsl:attribute name="identifierType" select="'DOI'" />
                                <xsl:value-of select="//datacite:alternateIdentifier[@alternateIdentifierType='Handle' and starts-with(., '11353/10.')]" />
                            </datacite:identifier>
                        </xsl:when>

                    </xsl:choose>


                </datacite:resource>

            </metadata>
        </record>
    </xsl:template>

</xsl:stylesheet>
<xsl:stylesheet xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dri="http://www.driver-repository.eu/namespace/dri" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:oai="http://www.openarchives.org/OAI/2.0/" xmlns:oaf="http://namespace.openaire.eu/oaf" xmlns:date="http://exslt.org/dates-and-times" xmlns:dr="http://www.driver-repository.eu/namespace/dr" xmlns:datacite="http://datacite.org/schema/kernel-4" version="2.0">
    <xsl:output indent="yes" />
    <xsl:param name="varOfficialName" />
    <xsl:param name="varDataSourceId" />
    <xsl:param name="quote">"</xsl:param>
    <xsl:variable name="baseURL" select="string('https://raw.githubusercontent.com/OpenAPC/openapc-de/master/data/apc_de.csv')" />

    <xsl:template name="terminate">
        <xsl:message terminate="yes"> record is not compliant, transformation is interrupted.
        </xsl:message>
    </xsl:template>
    <xsl:template match="/">
        <xsl:call-template name="oapcRecord" />
    </xsl:template>

    <xsl:template name="oapcRecord">
        <xsl:variable name="period" select="//column[./@name = 'period']/text()" />
        <xsl:variable name="institution" select="//column[./@name = 'institution']/text()" />
        <xsl:variable name="doi" select="//column[./@name = 'doi']/text()" />
        <xsl:variable name="euro" select="//column[./@name = 'euro']/text()" />
        <xsl:variable name="issn" select="//column[./@name = 'issn']/text()" />
        <xsl:variable name="jftitle" select="//column[./@name = 'journal_full_title']/text()" />
        <xsl:variable name="pmcid" select="//column[./@name = 'pmcid']/text()" />
        <xsl:variable name="pmid" select="//column[./@name = 'pmid']/text()" />
        <!-- xsl:variable name="gridid"         select="//column[./@name = 'grid_id']/text()"/>
        <xsl:variable name="rorid"          select="//column[./@name = 'ror_id']/text()"/ -->
        <xsl:variable name="license" select="//column[./@name = 'license_ref']/text()" />
        <oai:record>
            <oai:header>
                <xsl:copy-of copy-namespaces="no" select="//*[local-name() = 'header']/*" />
                <dr:dateOfTransformation>
                    <xsl:value-of select="date:date-time()" />
                </dr:dateOfTransformation>
            </oai:header>
            <metadata xmlns="http://namespace.openaire.eu/">
                <oaf:identifier identifierType="doi">
                    <xsl:value-of select="$doi" />
                </oaf:identifier>
                <oaf:identifier identifierType="pmcid">
                    <xsl:value-of select="$pmcid" />
                </oaf:identifier>
                <oaf:identifier identifierType="pmid">
                    <xsl:value-of select="$pmid" />
                </oaf:identifier>
                <!-- xsl:when test="//column[./@name = 'grid_id']/text()">
                    <datacite:affiliation affiliationIdentifier="$gridid" affiliationIdentifierScehme="GRID" SchemeURI="https://www.grid.ac/">
                         <xsl:value-of select="$institution" />
                    </datacite:affiliation>
              </xsl:when -->
                <oaf:processingchargeamount>
                    <xsl:attribute name="currency">EUR</xsl:attribute>
                    <xsl:value-of select="$euro" />
                </oaf:processingchargeamount>

                <oaf:journal>
                    <xsl:attribute name="issn">
                        <xsl:value-of select="$issn" />
                    </xsl:attribute>
                    <xsl:value-of select="$jftitle" />
                </oaf:journal>

                <dc:license>
                    <xsl:value-of select="$license" />
                </dc:license>
                <dc:date>
                    <xsl:value-of select="$period" />
                </dc:date>


                <dr:CobjCategory type="publication">0004</dr:CobjCategory>
                <oaf:accessrights>OPEN</oaf:accessrights>
                <datacite:rights rightsURI="http://purl.org/coar/access_right/c_abf2">open access</datacite:rights>

                <oaf:hostedBy>
                    <xsl:attribute name="name">Global OpenAPC Initiative</xsl:attribute>
                    <xsl:attribute name="id">openaire____::openapc_initiative</xsl:attribute>
                </oaf:hostedBy>
                <oaf:collectedFrom>
                    <xsl:attribute name="name">Global OpenAPC Initiative</xsl:attribute>
                    <xsl:attribute name="id">openaire____::openapc_initiative</xsl:attribute>
                </oaf:collectedFrom>
            </metadata>
            <oaf:about xmlns:oai="http://wwww.openarchives.org/OAI/2.0/">
                <oaf:datainfo>
                    <oaf:inferred>false</oaf:inferred>
                    <oaf:deletedbyinference>false</oaf:deletedbyinference>
                    <oaf:trust>0.9</oaf:trust>
                    <oaf:inferenceprovenance />
                    <oaf:provenanceaction classid="sysimport:crosswalk:datasetarchive" classname="sysimport:crosswalk:datasetarchive" schemeid="dnet:provenanceActions" schemename="dnet:provenanceActions" />
                </oaf:datainfo>
            </oaf:about>
        </oai:record>
    </xsl:template>

</xsl:stylesheet>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:oai="http://www.openarchives.org/OAI/2.0/"
                xmlns:oaf="http://namespace.openaire.eu/oaf"
                xmlns:eg="http://eu/dnetlib/trasform/extension"
                version="2.0"
                exclude-result-prefixes="xsl">
    <xsl:template match="/">
        <oai:record>
            <xsl:copy-of select="//oai:header"/>
            <metadata>
                <xsl:for-each select="//*[local-name()='subject']">
                    <subject><xsl:value-of select="eg:clean(.,'dnet:languages')"/></subject>
                </xsl:for-each>
            </metadata>
            <oaf:about>
                <oaf:datainfo>
                    <oaf:TestValue>incomplete</oaf:TestValue>
                    <oaf:provisionMode>collected</oaf:provisionMode>
                </oaf:datainfo>
            </oaf:about>
        </oai:record>
    </xsl:template>
</xsl:stylesheet>
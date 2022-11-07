<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:oai="http://www.openarchives.org/OAI/2.0/"
                xmlns:oaf="http://namespace.openaire.eu/oaf"
                xmlns:vocabulary="http://eu/dnetlib/transform/clean"
                xmlns:dr="http://www.driver-repository.eu/namespace/dr"
                version="2.0"
                exclude-result-prefixes="xsl vocabulary">
    <xsl:template match="/">

        <oai:record>
            <xsl:copy-of select="//oai:header"/>
            <metadata>

                <xsl:copy-of select="//oai:metadata/*"/>
                <xsl:for-each select="//oai:setSpec">
                    <dr:CobjCategory><xsl:value-of select="vocabulary:clean(.,'dnet:publication_resource')"/></dr:CobjCategory>
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
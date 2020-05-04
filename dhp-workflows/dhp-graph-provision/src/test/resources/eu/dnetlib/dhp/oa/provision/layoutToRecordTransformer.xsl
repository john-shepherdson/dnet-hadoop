<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:nxsl="http://www.w3.org/1999/XSL/TransformXX" version="3.0">
    <xsl:output method="xml" omit-xml-declaration="yes"/>
    <xsl:namespace-alias result-prefix="xsl" stylesheet-prefix="nxsl"/>
    <xsl:param name="format"/>
    <xsl:template match="/">
        <xsl:apply-templates select="//LAYOUT"/>
    </xsl:template>
    <xsl:template match="LAYOUT">
        <nxsl:stylesheet xmlns:dnet="http://www.d-net.research-infrastructures.eu/saxon-extension" xmlns:dri="http://www.driver-repository.eu/namespace/dri" exclude-result-prefixes="dnet" version="3.0">
            <nxsl:output method="xml" omit-xml-declaration="yes" version="1.0"/>
            <nxsl:variable name="format">
                <xsl:value-of select="$format"/>
            </nxsl:variable>
            <nxsl:template match="/">
                <indexRecord>
                    <indexRecordIdentifier>
                        <nxsl:value-of select="//dri:objIdentifier"/>
                    </indexRecordIdentifier>
                    <targetFields>
                        <nxsl:if test="count(//*[local-name()='metadata']/*) &gt; 0">
                            <xsl:apply-templates select="FIELDS/FIELD[@indexable='true']"/>
                        </nxsl:if>
                    </targetFields>
                    <dnetResult>
                        <nxsl:copy-of select="/*[local-name()='record']/*[local-name()='result']/*[local-name()='header']"/>
                        <metadata>
                            <xsl:apply-templates mode="result" select="FIELDS/FIELD"/>
                        </metadata>
                    </dnetResult>
                </indexRecord>
            </nxsl:template>
        </nxsl:stylesheet>
    </xsl:template>
    <xsl:template match="FIELD[@indexable='true']">
        <xsl:choose>
            <xsl:when test="@constant">
                <xsl:element name="{@name}">
                    <xsl:value-of select="@constant"/>
                </xsl:element>
            </xsl:when>
            <xsl:when test="@value and not(@xpath)">
                <xsl:choose>
                    <xsl:when test="contains(@type, 'date')">
                        <nxsl:variable name="{@name}" select="dnet:normalizeDate(normalize-space({normalize-space(@value)}))"/>
                        <nxsl:if test="string-length(${@name}) &gt; 0">
                            <nxsl:element name="{@name}">
                                <nxsl:value-of select="${@name}"/>
                            </nxsl:element>
                        </nxsl:if>
                    </xsl:when>
                    <xsl:otherwise>
                        <nxsl:element name="{@name}">
                            <nxsl:value-of select="{@value}"/>
                        </nxsl:element>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
            <xsl:otherwise>
                <xsl:variable name="value">
                    <xsl:choose>
                        <xsl:when test="@value">
                            <xsl:value-of select="@value"/>
                        </xsl:when>
                        <xsl:otherwise>
                            .
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <nxsl:for-each select="{@xpath}">
                    <xsl:element name="{@name}">
                        <xsl:choose>
                            <xsl:when test="@tokenizable='false'">
                                <nxsl:value-of select="normalize-space({normalize-space($value)})"/>
                            </xsl:when>
                            <xsl:otherwise>
                                <nxsl:value-of select="{normalize-space($value)}"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:element>
                </nxsl:for-each>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    <xsl:template match="FIELD" mode="result">
        <xsl:if test="@result='true'">
            <nxsl:copy-of select="{@xpath}"/>
        </xsl:if>
    </xsl:template>
    <xsl:template match="FIELD" mode="header">
        <xsl:if test="@header='true'">
            <nxsl:copy-of select="{@xpath}"/>
        </xsl:if>
    </xsl:template>
</xsl:stylesheet>
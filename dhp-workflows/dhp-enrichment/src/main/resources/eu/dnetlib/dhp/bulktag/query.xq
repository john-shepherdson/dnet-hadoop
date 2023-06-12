for $x in collection('/db/DRIVER/ContextDSResources/ContextDSResourceType')
let $subj := $x//CONFIGURATION/context/param[./@name='subject']/text()
let $datasources := $x//CONFIGURATION/context/category[./@id=concat($x//CONFIGURATION/context/@id,'::contentproviders')]/concept
let $organizations := $x//CONFIGURATION/context/category[./@id=concat($x//CONFIGURATION/context/@id,'::resultorganizations')]/concept
let $communities := $x//CONFIGURATION/context/category[./@id=concat($x//CONFIGURATION/context/@id,'::zenodocommunities')]/concept
let $fos := $x//CONFIGURATION/context/param[./@name='fos']/text()
let $sdg := $x//CONFIGURATION/context/param[./@name='sdg']/text()
let $zenodo := $x//param[./@name='zenodoCommunity']/text()
where $x//CONFIGURATION/context[./@type='community' or ./@type='ri'] and $x//context/param[./@name = 'status']/text() != 'hidden'
return
<community>
{ $x//CONFIGURATION/context/@id}
<removeConstraints>
{$x//CONFIGURATION/context/param[./@name='removeConstraints']/text() }
</removeConstraints>
<advancedConstraints>
{$x//CONFIGURATION/context/param[./@name='advancedConstraints']/text() }
</advancedConstraints>
 <subjects>
 {for $y in tokenize($subj,',')
 return
 <subject>{$y}</subject>}
 {for $y in tokenize($fos,',')
 return
 <subject>{$y}</subject>}
 {for $y in tokenize($sdg,',')
 return
 <subject>{$y}</subject>}
 </subjects>
 <datasources>
 {for $d in $datasources
 where $d/param[./@name='enabled']/text()='true'
 return
 <datasource>
 <openaireId>
 {$d//param[./@name='openaireId']/text()}
 </openaireId>
 <selcriteria>
 {$d/param[./@name='selcriteria']/text()}
 </selcriteria>
 </datasource> }
 </datasources>
  <zenodocommunities>
{for $zc in $zenodo
return
<zenodocommunity>
<zenodoid>
{$zc}
</zenodoid>
</zenodocommunity>}
{for $zc in $communities
return
<zenodocommunity>
<zenodoid>
{$zc/param[./@name='zenodoid']/text()}
</zenodoid>
<selcriteria>
{$zc/param[./@name='selcriteria']/text()}
</selcriteria>
</zenodocommunity>}
</zenodocommunities>
</community>
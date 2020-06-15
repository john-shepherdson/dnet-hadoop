for $x in collection('/db/DRIVER/VocabularyDSResources/VocabularyDSResourceType')
let $vocid := $x//VOCABULARY_NAME/@code
let $vocname := $x//VOCABULARY_NAME/text()
	for $term in ($x//TERM)
		for $syn in ($term//SYNONYM/@term)
			return concat($vocid,' @=@ ',$term/@code,' @=@ ', $syn)

package pure_ops
package implementation

final case object AWSet extends OWSet {
	def combine_ARSet(ar_set0: ARSet,
	                  ar_set1: ARSet):
	ARSet = {
		val aset = ar_set0.aset ++ ar_set1.aset
		val rset = (ar_set0.rset ++ ar_set1.rset) -- aset
		ARSet(aset, rset)		
	}
}
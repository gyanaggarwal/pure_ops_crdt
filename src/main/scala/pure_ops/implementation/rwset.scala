package pure_ops
package implementation

final case object RWSet extends OWSet {	
  def combine_ARSet(ar_set0: ARSet,
	            ar_set1: ARSet):
  ARSet = {
    val rset = ar_set0.rset ++ ar_set1.rset
    val aset = (ar_set0.aset ++ ar_set1.aset) -- rset
    ARSet(aset, rset)		
  }
}

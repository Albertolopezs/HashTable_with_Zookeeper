package es.upm.dit.dscc.DHT;

public class Prueba {
	
	int nServersMax = 3;
	
	public Prueba() {
		
	}
	
	public int getPos (int hash) {
		int segment = Integer.MAX_VALUE / (nServersMax); // No negatives

		//		if (hash < segment     && hash > 0)           return 0;
		//		if (hash < segment * 2 && hash > segment)     return 1;
		//		if (hash < segment * 3 && hash > segment * 2) return 2;

		for(int i = 0; i < nServersMax; i++) {
			if (hash >= (i * segment) && (hash <  (i+1)*segment)){
				return i;
			}
		}

		return 1;
		
	}
	
	public static void main(String[] args) {
		
		Prueba pr = new Prueba();
		int hash = 5;
		int segment = Integer.MAX_VALUE / (pr.nServersMax);
		
		System.out.println("Pos: " + pr.getPos(hash) +  " Value: " + hash);
		
		hash = hash + segment;
		System.out.println("Pos: " + pr.getPos(hash) +  " Value: " + hash);
		
		hash = hash + segment;
		System.out.println("Pos: " + pr.getPos(hash) +  " Value: " + hash);
		System.out.println(Integer.MAX_VALUE);
		
		
	}
	
}

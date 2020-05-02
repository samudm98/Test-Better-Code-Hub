package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma;

import co.gov.ugpp.parafiscales.servicios.liquidador.entity.Nomina;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.NominaDetalle;

public class DatosEjecucionRegla implements java.io.Serializable
{
   private static final long serialVersionUID = -5366154886033571058L;

   private NominaDetalle nominaDetalle;
   
   private Nomina nomina;
   

   public DatosEjecucionRegla()
   {

   }

   public NominaDetalle getNominaDetalle()
   {
      return nominaDetalle;
   }

   public void setNominaDetalle(NominaDetalle nominaDetalle)
   {
      this.nominaDetalle = nominaDetalle;
   }

   public Nomina getNomina()
   {
      return nomina;
   }

   public void setNomina(Nomina nomina)
   {
      this.nomina = nomina;
   }



}

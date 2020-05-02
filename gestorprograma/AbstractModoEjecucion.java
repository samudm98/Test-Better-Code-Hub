package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.OpLiquidarSolTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.PilaDepurada;
import co.gov.ugpp.parafiscales.servicios.liquidador.errortipo.v1.ErrorTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.entity.InRegla;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.jpa.GestorProgramaDao;

public abstract class AbstractModoEjecucion implements Serializable
{

   private static final long serialVersionUID = -7619482210924918310L;

   public AbstractModoEjecucion()
   {

   }

   public abstract List<DatosEjecucionRegla> getDatosEjecucionRegla(GestorProgramaDao gestorProgramaDao,
         OpLiquidarSolTipo msjOpLiquidarSol);

   /**
    * Variables que deben ser reemplazadas por sus valores en los objetos java
    * 
    * @param scriptRegla
    * @param obj
    * @return
    */
   public abstract String inyectarValoresRegla(String scriptRegla, DatosEjecucionRegla obj, Map<String, Object> mapVariablesRegla);

   /**
    * Coloca el valor de las variables encontradas para las reglas en el script
    * 
    * @param scriptRegla
    * @param obj
    * @param mapVariablesRegla
    * @return
    */
   public abstract String reemplazarVariablesRegla(String scriptRegla, DatosEjecucionRegla obj,
         Map<String, Object> mapVariablesRegla);

   public abstract Object buscarVariablesRegla(List<ErrorTipo> errorTipo, GestorProgramaDao gestorProgramaDao,
         DatosEjecucionRegla obj, InRegla inRegla, Map<String, Object> mapVariablesRegla);

   public abstract void procesarReglasNoFormula(List<ErrorTipo> errorTipo, GestorProgramaDao gestorProgramaDao,
         DatosEjecucionRegla obj, Map<String, Object> infoNegocio, PilaDepurada pilaDetalle);

   public String convertDateToString(Date date)
   {
      try
      {
         DateFormat df = new SimpleDateFormat("MM/dd/yyyy");

         // Get the date today using Calendar object.

         // Using DateFormat format method we can create a string
         // representation of a date with the defined format.
         return df.format(date);

      }
      catch (Exception e)
      {

      }

      return "";

   }

   public String getNumberToString(Number number)
   {
      if (number == null)
         return "nil";
      else
         return number.toString();
   }

   /**
    * Suma los valores ingresados en el orden que se recibe por parametros
    * 
    * @param datos
    * @return
    */
   public BigDecimal sumValorReglas(Object... datos)
   {
      BigDecimal result = new BigDecimal("0");

      for (int i = 0; i < datos.length; i++)
      {
         if (datos[i] instanceof BigDecimal)
         {
            result = result.add((BigDecimal) datos[i]);
         }
         else if (datos[i] instanceof Double)
         {
            result = result.add(new BigDecimal((Double) datos[i]));
         }
         if (datos[i] instanceof Integer)
         {
            result = result.add(new BigDecimal((Integer) datos[i]));
         }
      }

      return result;
   }

   /**
    * Multiplica los valores ingresados en el orden que se recibe por parametros
    * 
    * @param datos
    * @return
    */
   public BigDecimal mulValorReglas(Object... datos)
   {
      BigDecimal result = new BigDecimal("0");

      for (int i = 0; i < datos.length; i++)
      {
         if (i == 0)
         {
            if (datos[i] instanceof BigDecimal)
            {
               result = (BigDecimal) datos[i];
            }
            else if (datos[i] instanceof Double)
            {
               result = new BigDecimal((Double) datos[i]);
            }
            if (datos[i] instanceof Integer)
            {
               result = new BigDecimal((Integer) datos[i]);
            }
            if (datos[i] instanceof String)
            {
               result = new BigDecimal((String) datos[i]);
            }
            
         }
         else if (datos[i] instanceof BigDecimal)
         {
            result = result.multiply((BigDecimal) datos[i]);
         }
         else if (datos[i] instanceof Double)
         {
            result = result.multiply(new BigDecimal((Double) datos[i]));
         }
         else if (datos[i] instanceof Integer)
         {
            result = result.multiply(new BigDecimal((Integer) datos[i]));
         }
         else if (datos[i] instanceof String)
         {
            result = result.multiply(new BigDecimal((String) datos[i]));
         }
      }

      return result;
   }



   
   public BigDecimal minusValorReglas(Object... datos)
   {
      BigDecimal result = BigDecimal.ZERO;

      //verificar porque no hace la resta fd elos valores y devuelve un valor loco.......datoshay voy
      //colocar sytem a todos eso if a ver donde entra.....
      
      for (int i = 0; i < datos.length; i++)
      {

         if (i == 0)
         {
            if (datos[i] instanceof BigDecimal)
            {
               result = (BigDecimal) datos[i];
            }
            else if (datos[i] instanceof Double)
            {
               result = new BigDecimal((Double) datos[i]);
            }
            if (datos[i] instanceof Integer)
            {
               result = new BigDecimal((Integer) datos[i]);
            }
            if (datos[i] instanceof String)
            {
               result = new BigDecimal((String) datos[i]);
            }
            
            
         }
         else
         {
            if (datos[i] instanceof BigDecimal)
            {
               result = result.subtract((BigDecimal) datos[i]);
            }
            else if (datos[i] instanceof Double)
            {
               result = result.subtract(new BigDecimal((Double) datos[i]));
            }
            if (datos[i] instanceof Integer)
            {
               result = result.subtract(new BigDecimal((Integer) datos[i]));
            }
            if (datos[i] instanceof String)
            {
               result = result.subtract(new BigDecimal((String) datos[i]));
            }
            
            
         }
      }

      return result;
   }
   
   
   
   
   
   public BigDecimal convertValorRegla(Object valor)
   {
      BigDecimal result = BigDecimal.ZERO;

      if (valor instanceof BigDecimal)
         return (BigDecimal) valor;
      else if (valor instanceof Double)
         return new BigDecimal((Double) valor);
      if (valor instanceof Integer)
         return new BigDecimal((Integer) valor);
      if (valor instanceof String)
         return new BigDecimal((String) valor);
      
      return result;
   }
   
   
   public BigDecimal roundValor(BigDecimal valor)
   {
      BigDecimal bd = valor.setScale(0, BigDecimal.ROUND_UP);

      return bd;
   }

   
   
   public BigDecimal roundValor1000(BigDecimal valor)
   {
      try
      {
         return new BigDecimal(valor.setScale(-3, BigDecimal.ROUND_UP).doubleValue());
      }
      catch (NumberFormatException ex)
      {
         System.out.println("::ANDRES:: NumberFormatException roundValor1000: " + ex);
         
         if (Double.isInfinite(valor.doubleValue()))
         {
            return valor;
         }
         else
         {
            return new BigDecimal(Double.NaN);
         }
      }
   }

   public BigDecimal roundValor100(BigDecimal valor)
   {
      try
      {
         return new BigDecimal(valor.setScale(-2, BigDecimal.ROUND_UP).doubleValue());
      }
      catch (NumberFormatException ex)
      {
         System.out.println("::ANDRES:: NumberFormatException roundValor100: " + ex);
         
         if (Double.isInfinite(valor.doubleValue()))
         {
            return valor;
         }
         else
         {
            return new BigDecimal(Double.NaN);
         }
      }
   }
   

   public String anyoMesDetalleKey(DatosEjecucionRegla obj)
   {
      return "#" + obj.getNominaDetalle().getAno().toString() + obj.getNominaDetalle().getMes().toString();
   }   
   
   public abstract void cerrarLstAdministradoraPila();
   
}

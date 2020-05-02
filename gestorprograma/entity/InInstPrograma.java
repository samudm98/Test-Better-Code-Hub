package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.entity;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

@Entity
@Table(name = "in_inst_programa")
public class InInstPrograma implements java.io.Serializable
{
   private static final long serialVersionUID = 8342566815603109288L;
   private String idInstanciaPrograma;
   private String idPrograma;
   private String idUsuarioCreacion;
   private String idUsuarioModificacion;
   private Date fecCreacionRegistro;
   private Date fecModificacionRegistro;

   public InInstPrograma()
   {
   }

   @Id
   @Column(name = "id_instancia_programa", unique = true, nullable = false, length = 50)
   @NotNull
   @Size(max = 50)
   public String getIdInstanciaPrograma()
   {
      return this.idInstanciaPrograma;
   }

   public void setIdInstanciaPrograma(String idInstanciaPrograma)
   {
      this.idInstanciaPrograma = idInstanciaPrograma;
   }

   @Column(name = "id_programa", nullable = false, length = 50)
   @NotNull
   @Size(max = 50)
   public String getIdPrograma()
   {
      return this.idPrograma;
   }

   public void setIdPrograma(String idPrograma)
   {
      this.idPrograma = idPrograma;
   }

   @Column(name = "id_usuario_creacion", length = 50)
   @Size(max = 50)
   public String getIdUsuarioCreacion()
   {
      return this.idUsuarioCreacion;
   }

   public void setIdUsuarioCreacion(String idUsuarioCreacion)
   {
      this.idUsuarioCreacion = idUsuarioCreacion;
   }

   @Column(name = "id_usuario_modificacion", length = 50)
   @Size(max = 50)
   public String getIdUsuarioModificacion()
   {
      return this.idUsuarioModificacion;
   }

   public void setIdUsuarioModificacion(String idUsuarioModificacion)
   {
      this.idUsuarioModificacion = idUsuarioModificacion;
   }

   @Temporal(TemporalType.TIMESTAMP)
   @Column(name = "fec_creacion_registro", length = 19)
   public Date getFecCreacionRegistro()
   {
      return this.fecCreacionRegistro;
   }

   public void setFecCreacionRegistro(Date fecCreacionRegistro)
   {
      this.fecCreacionRegistro = fecCreacionRegistro;
   }

   @Temporal(TemporalType.TIMESTAMP)
   @Column(name = "fec_modificacion_registro", length = 19)
   public Date getFecModificacionRegistro()
   {
      return this.fecModificacionRegistro;
   }

   public void setFecModificacionRegistro(Date fecModificacionRegistro)
   {
      this.fecModificacionRegistro = fecModificacionRegistro;
   }

}

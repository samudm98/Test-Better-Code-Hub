package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.entity;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

@Entity
@Table(name = "in_regla")
public class InRegla implements java.io.Serializable
{

   private static final long serialVersionUID = -7083735375699825366L;
   private String id;
   private String nombre;
   private String descripcion;
   private String codigo;
   private String cod_tipo;
   private byte[] valScript;
   private String idUsuarioCreacion;
   private String idUsuarioModificacion;
   private Date fecCreacionRegistro;
   private Date fecModificacionRegistro;

   public InRegla()
   {
   }

   @Id
   @Column(name = "id", unique = true, nullable = false, length = 50)
   @NotNull
   @Size(max = 50)
   public String getId()
   {
      return this.id;
   }

   public void setId(String id)
   {
      this.id = id;
   }

   @Column(name = "nombre", nullable = false, length = 50)
   @NotNull
   @Size(max = 50)
   public String getNombre()
   {
      return this.nombre;
   }

   public void setNombre(String nombre)
   {
      this.nombre = nombre;
   }

   @Column(name = "descripcion", length = 500)
   @Size(max = 500)
   public String getDescripcion()
   {
      return descripcion;
   }

   public void setDescripcion(String descripcion)
   {
      this.descripcion = descripcion;
   }

   @Column(name = "codigo", nullable = false, length = 200)
   @NotNull
   @Size(max = 200)
   public String getCodigo()
   {
      return codigo;
   }

   public void setCodigo(String codigo)
   {
      this.codigo = codigo;
   }

   @Column(name = "cod_tipo_script", length = 10)
   @NotNull
   @Size(max = 10)
   public String getCod_tipo()
   {
      return cod_tipo;
   }

   public void setCod_tipo(String cod_tipo)
   {
      this.cod_tipo = cod_tipo;
   }

   @Column(name = "val_script", nullable = false)
   @Lob
   @NotNull
   public byte[] getValScript()
   {
      return this.valScript;
   }

   public void setValScript(byte[] valScript)
   {
      this.valScript = valScript;
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

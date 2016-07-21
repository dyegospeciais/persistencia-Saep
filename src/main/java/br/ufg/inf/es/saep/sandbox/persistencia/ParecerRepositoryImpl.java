package br.ufg.inf.es.saep.sandbox.persistencia;

import br.ufg.inf.es.saep.sandbox.dominio.Avaliavel;
import br.ufg.inf.es.saep.sandbox.dominio.CampoExigidoNaoFornecido;
import br.ufg.inf.es.saep.sandbox.dominio.IdentificadorDesconhecido;
import br.ufg.inf.es.saep.sandbox.dominio.IdentificadorExistente;
import br.ufg.inf.es.saep.sandbox.dominio.Nota;
import br.ufg.inf.es.saep.sandbox.dominio.Parecer;
import br.ufg.inf.es.saep.sandbox.dominio.ParecerRepository;
import br.ufg.inf.es.saep.sandbox.dominio.Radoc;
import com.google.gson.Gson; //biblioteca do google para trabalhar com "Json"
import com.google.gson.GsonBuilder;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 * Classe que implementa a persistência segundo a interface
 * {@code ParecerRepository}.
 * 
 * @see ParecerRepository
 */
public class ParecerRepositoryImpl implements ParecerRepository {

	private final MongoDatabase db;
	static Gson gson = new GsonBuilder().serializeNulls().create();

	/**
	 * Construtor da Implementação da interface {@link ParecerRepository}.
	 * 
	 * @param db
	 *            Uma instancia do banco MongoDB onde será feira a persistência.
	 */
	public ParecerRepositoryImpl(MongoDatabase db) {
		if (db == null) {
			throw new CampoExigidoNaoFornecido("db");
		}

		this.db = db;
	}

	@Override
	public void adicionaNota(String id, Nota nota) {
		Iterable<Document> it = db.getCollection(
				MongoConstantes.PARECERES_COLLECTION).find(
						new Document("_id", id));

		if (it.iterator().hasNext()) {
			db.getCollection(MongoConstantes.PARECERES_COLLECTION).updateOne(
					new Document("_id", id), // encontrar o parecer correto
					new Document("$push", // fazer a adiçao de um novo documento
							// no array
							new Document("notas", Document.parse(gson
									.toJson(nota)))) // dizer o array e o que
					// dever ser adicionado
					);
		} else {
			throw new IdentificadorDesconhecido("id");
		}
	}

	@Override
	public void removeNota(String id, Avaliavel original) {
		Iterable<Document> it = db.getCollection(
				MongoConstantes.PARECERES_COLLECTION).find(
						new Document("_id", id));

		if (it.iterator().hasNext()) {
			Document d = it.iterator().next();
			Parecer p = gson.fromJson(d.toJson().replaceFirst("_id", "id"),
					Parecer.class);
			for (Nota nota : p.getNotas()) {
				if (nota.getItemOriginal().equals(original)) {

					db.getCollection(MongoConstantes.PARECERES_COLLECTION)
					.updateOne(
							new Document("_id", id),
							new Document("$pull", // fazer a retirada da
									// nota no array
									new Document("notas", Document
											.parse(gson.toJson(nota)))));
				}
			}
		}
	}

	@Override
	public void persisteParecer(Parecer parecer) {
		Iterable<Document> it = db.getCollection(
				MongoConstantes.PARECERES_COLLECTION).find(
						new Document("_id", parecer.getId()));

		if (it.iterator().hasNext())
			throw new IdentificadorExistente("id");

		String parecerJson = gson.toJson(parecer);
		parecerJson = parecerJson.replaceFirst("id", "_id");

		Document parecerDocument = Document.parse(parecerJson);

		db.getCollection(MongoConstantes.PARECERES_COLLECTION).insertOne(
				parecerDocument);
	}

	@Override
	public void atualizaFundamentacao(String parecer, String fundamentacao) {

		Iterable<Document> it = db.getCollection(
				MongoConstantes.PARECERES_COLLECTION).find(
						new Document("_id", parecer));

		if (it.iterator().hasNext()) {
			db.getCollection(MongoConstantes.PARECERES_COLLECTION).updateOne(
					new Document("_id", parecer),
					new Document("fundamentacao", fundamentacao));
		} else {
			throw new IdentificadorDesconhecido("parecer");
		}
	}

	@Override
	public Parecer byId(String id) {
		Iterable<Document> it = db.getCollection(
				MongoConstantes.PARECERES_COLLECTION).find(
						new Document("_id", id));

		if (it.iterator().hasNext()) {
			Document d = it.iterator().next();
			return gson.fromJson(d.toJson().replaceFirst("_id", "id"),
					Parecer.class);
		}
		return null;
	}

	@Override
	public void removeParecer(String id) {
		db.getCollection(MongoConstantes.PARECERES_COLLECTION).deleteOne(
				new Document("_id", id));
	}

	@Override
	public Radoc radocById(String identificador) {
		Iterable<Document> it = db.getCollection(
				MongoConstantes.RADOCS_COLLECTION).find(
						new Document("_id", identificador));

		if (it.iterator().hasNext()) {
			Document d = it.iterator().next();
			return gson.fromJson(d.toJson().replaceFirst("_id", "id"),
					Radoc.class);
		}
		return null;
	}

	@Override
	public String persisteRadoc(Radoc radoc) {
		Iterable<Document> it = db.getCollection(
				MongoConstantes.RADOCS_COLLECTION).find(
						new Document("_id", radoc.getId()));

		if (it.iterator().hasNext())
			throw new IdentificadorExistente("id");

		String radocJson = gson.toJson(radoc);
		radocJson = radocJson.replaceFirst("id", "_id");

		Document radocDocument = Document.parse(radocJson);
		db.getCollection(MongoConstantes.RADOCS_COLLECTION).insertOne(
				radocDocument);
		return radoc.getId();
	}

	@Override
	public void removeRadoc(String identificador) {
		// Procura na coleçao de Pareceres se alguma delas possui referencia a
		// esse Radoc
		Iterable<Document> it = db.getCollection(
				MongoConstantes.PARECERES_COLLECTION).find(
						new Document("radocs", identificador));

		if (it.iterator().hasNext()){	
			return; // Nao e permitido remover um Radoc que e referenciado por
					// algum Parecer
		}
		db.getCollection(MongoConstantes.RADOCS_COLLECTION).deleteOne(
				new Document("_id", identificador));
	}
	
}
